package main

import (
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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

const (
	maxRetries = 5
	taintKey   = "spot-interruption"
)

type Controller struct {
	queue      workqueue.TypedRateLimitingInterface[string]
	nodeLister listersv1.NodeLister
	podLister  listersv1.PodLister
	hasSynced  []cache.InformerSynced
}

func NewController(factory informers.SharedInformerFactory) *Controller {
	nodeInformer := factory.Core().V1().Nodes()
	podInformer := factory.Core().V1().Pods()

	c := &Controller{
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
		return nil
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

		owner := metav1.GetControllerOf(pod)
		switch {
		case owner == nil:
			fmt.Printf("  %s/%s — bare pod, will NOT be recreated\n",
				pod.Namespace, pod.Name)
			atRisk++
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

func main() {
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
	controller := NewController(factory)

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
