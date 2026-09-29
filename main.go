package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const (
	maxRetries     = 5
	taintKey       = "spot-interruption"
	riskAnnotation = "node-drain-controller/at-risk"
)

type Controller struct {
	clientset  kubernetes.Interface
	queue      workqueue.TypedRateLimitingInterface[string]
	nodeLister listersv1.NodeLister
	podLister  listersv1.PodLister
	hasSynced  []cache.InformerSynced
	recorder   record.EventRecorder
	dryRun     bool
}

func NewController(
	clientset kubernetes.Interface,
	factory informers.SharedInformerFactory,
	dryRun bool,
) *Controller {
	nodeInformer := factory.Core().V1().Nodes()
	podInformer := factory.Core().V1().Pods()

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: clientset.CoreV1().Events(""),
	})
	recorder := broadcaster.NewRecorder(
		scheme.Scheme,
		corev1.EventSource{Component: "node-drain-controller"},
	)

	c := &Controller{
		clientset: clientset,
		dryRun:    dryRun,
		recorder:  recorder,
		queue: workqueue.NewTypedRateLimitingQueue[string](
			workqueue.DefaultTypedControllerRateLimiter[string](),
		),
		nodeLister: nodeInformer.Lister(),
		podLister:  podInformer.Lister(),
		hasSynced: []cache.InformerSynced{
			nodeInformer.Informer().HasSynced,
			podInformer.Informer().HasSynced,
		},
	}

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueue,
		UpdateFunc: func(oldObj, newObj interface{}) { c.enqueue(newObj) },
		DeleteFunc: c.enqueue,
	})

	return c
}

func (c *Controller) enqueue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		fmt.Printf("could not build key: %v\n", err)
		return
	}
	c.queue.Add(key)
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	if !cache.WaitForCacheSync(stopCh, c.hasSynced...) {
		fmt.Println("failed to sync caches")
		return
	}
	fmt.Println("caches synced — starting workers")

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	fmt.Println("stopping workers")
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

func (c *Controller) processNextItem() bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	err := c.reconcile(key)
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	if c.queue.NumRequeues(key) < maxRetries {
		fmt.Printf("error reconciling %s: %v — retrying\n", key, err)
		c.queue.AddRateLimited(key)
		return true
	}

	fmt.Printf("giving up on %s after %d attempts: %v\n", key, maxRetries, err)
	c.queue.Forget(key)
	return true
}

func (c *Controller) reconcile(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	node, err := c.nodeLister.Get(name)
	if apierrors.IsNotFound(err) {
		fmt.Printf("node %s no longer exists\n", name)
		return nil
	}
	if err != nil {
		return err
	}

	if node.DeletionTimestamp != nil {
		fmt.Printf("node %s is terminating, skipping\n", name)
		return nil
	}

	var taint *corev1.Taint
	for i := range node.Spec.Taints {
		if node.Spec.Taints[i].Key == taintKey {
			taint = &node.Spec.Taints[i]
			break
		}
	}

	if taint == nil {
		return c.clearAnnotations(name)
	}

	fmt.Printf("\nnode %s tainted: %s=%s:%s\n",
		name, taint.Key, taint.Value, taint.Effect)

	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return err
	}

	var atRisk int
	for _, pod := range pods {
		if pod.Spec.NodeName != name {
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		if _, isMirror := pod.Annotations["kubernetes.io/config.mirror"]; isMirror {
			continue
		}

		owner := metav1.GetControllerOf(pod)
		switch {
		case owner == nil:
			fmt.Printf("  %s/%s — bare pod, will NOT be recreated\n",
				pod.Namespace, pod.Name)
			atRisk++
			c.recorder.Eventf(pod, corev1.EventTypeWarning, "DisruptionRisk",
				"Node %s is marked for disruption and this pod has no controller owner, so it will not be recreated",
				name)
			if err := c.annotatePod(pod, name); err != nil {
				return err
			}
		case owner.Kind == "DaemonSet":
			fmt.Printf("  %s/%s — daemonset, expected\n",
				pod.Namespace, pod.Name)
		default:
			fmt.Printf("  %s/%s — owned by %s %s\n",
				pod.Namespace, pod.Name, owner.Kind, owner.Name)
		}
	}

	if atRisk > 0 {
		fmt.Printf("  %d pod(s) at risk on %s\n", atRisk, name)
	}
	return nil
}

func (c *Controller) annotatePod(pod *corev1.Pod, nodeName string) error {
	if pod.Annotations[riskAnnotation] == nodeName {
		return nil
	}

	if c.dryRun {
		fmt.Printf("    [dry-run] would annotate %s/%s\n", pod.Namespace, pod.Name)
		return nil
	}

	patch := []byte(fmt.Sprintf(
		`{"metadata":{"annotations":{%q:%q}}}`, riskAnnotation, nodeName))

	_, err := c.clientset.CoreV1().Pods(pod.Namespace).Patch(
		context.Background(),
		pod.Name,
		types.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("annotating %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	fmt.Printf("    annotated %s/%s\n", pod.Namespace, pod.Name)
	return nil
}

func (c *Controller) clearAnnotations(nodeName string) error {
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return err
	}

	for _, pod := range pods {
		if pod.Spec.NodeName != nodeName {
			continue
		}
		if pod.Annotations[riskAnnotation] == "" {
			continue
		}
		if err := c.removeAnnotation(pod); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) removeAnnotation(pod *corev1.Pod) error {
	if c.dryRun {
		fmt.Printf("[dry-run] would clear annotation on %s/%s\n",
			pod.Namespace, pod.Name)
		return nil
	}

	patch := []byte(fmt.Sprintf(
		`{"metadata":{"annotations":{%q:null}}}`, riskAnnotation))

	_, err := c.clientset.CoreV1().Pods(pod.Namespace).Patch(
		context.Background(),
		pod.Name,
		types.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("clearing annotation on %s/%s: %w",
			pod.Namespace, pod.Name, err)
	}

	fmt.Printf("cleared annotation on %s/%s — node no longer tainted\n",
		pod.Namespace, pod.Name)
	return nil
}

func main() {
	dryRun := flag.Bool("dry-run", false, "log actions without changing anything")
	flag.Parse()

	kubeconfig := filepath.Join(homeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	factory := informers.NewSharedInformerFactory(clientset, 30*time.Second)
	controller := NewController(clientset, factory, *dryRun)

	if *dryRun {
		fmt.Println("running in dry-run mode — no changes will be made")
	}

	stopCh := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("\nshutting down")
		close(stopCh)
	}()

	factory.Start(stopCh)
	controller.Run(2, stopCh)
}

func homeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		panic("could not determine home directory")
	}
	return home
}
