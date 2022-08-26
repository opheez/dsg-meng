# DprCounters Example

This is a basic DPR example. Here, we are using libDPR to add DPR semantics to a cluster of counter servers --- one
counter per server. You can find the server implementation in `CounterServer.cs` and `CounterStateObject.cs`;
client implementation is in `CounterClient.cs` and `CounterClientSession.cs`.

We are also providing all the necessary code to deploy a cluster consisting of a specified number of servers 
with specified characteristics, such as memory and storage. We abstract away almost the entire logic behind cluster
formation and only expect the user to specify how they want the cluster to look like. The cluster is deployed and
coordinated by Kubernetes.

The next few sections cover the basics of setting up your machine to be able to deploy the code and how to actually
deploy the cluster. In this README, we are assuming you are currently at `faster/cs/libdpr/samples/DprCounters/DprCounters`.

## Setup

We've used WSL2 for Windows for developing this project and it is also what we used to deploy and test the clusters.
Hence, we currently support Ubuntu for deploying and interacting with the cluster locally. In addition, you need to
have installed `kubectl`, `minikube` and `docker` on your machine. Note that each of these is absolutely necessary
in order to deploy a local Kubernetes cluster, so this is actually very lightweight.

In order to install `minikube` please refer to this [guide](https://minikube.sigs.k8s.io/docs/start/.).

To install `kubectl` you can follow [this link](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/).

Finally, click [here](https://www.simplilearn.com/tutorials/docker-tutorial/how-to-install-docker-on-ubuntu) for a 
guide to installing Docker.

On a machine that has all of the above installed, simply run:
```
./scripts/startup.sh
```
and your environment will be set up.

After any modification to the code, or the first time you set up the environment run:
```
./scripts/docker_stuff.sh
```

The startup script and the docker script need to be run once per terminal window! If the image is not getting updated
for you, it is quite likely that you are using a new terminal in which you haven't run the scripts.

## Deploying the cluster

Deploying the cluster is really simple. Simply go to `python/start_cluster.py` and modify the main function. Currently,
the main function looks like:

```
def main():
    cluster = KubernetesCluster()
    cluster.addServer("counter")
    cluster.addServer("counter")
    cluster.start()
```

This piece of code is pretty self-explanatory and it starts a cluster with two counter servers. The client can then be
run independently, as discussed below.

The only required field in the `addServer()` method is the type of the server and you can only deploy supported servers.
In addition, it is possible to many other attributes such as specify storage, memory, limits, storage location in the method.
Running the code will then start the server for you, same as in the simple example, and there is nothing else required.

An example of how to specify some of the resources would look like this:
```
    cluster = KubernetesCluster()
    cluster.addServer("counter", cpu_request="100Mi", cpu_limit="1Gi", memory_request = "100Mi", memory_limit = "1Gi")
    cluster.addServer("counter", cpu_request="200Mi", cpu_limit="2Gi", memory_request = "200Mi", memory_limit = "2Gi")
    cluster.start()
```

## Starting the client

The client is implemented in `Program.cs`. TODO(Nikola): Finish this once the client side is a bit nicer