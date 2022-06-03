from ctypes import ArgumentError
from posixpath import basename
from kubernetes import client, config
import os
import time
from numpy import void

import yaml


class KubernetesCluster():

    DEFAULT_STORAGE = "1Gi"
    SUPPORTED_SERVER_TYPES = ["counter"]

    class Server():

        def __init__(self, type:str, storage:str = None, storage_location:str = None, memory:str = None, limit:str = None):
            self.type = type
            self.storage = storage
            self.storage_location = storage_location
            self.memory = memory
            self.limit = limit

    def __init__(self, checkpoint_dir:str = None) -> void:
        config.load_kube_config()
        self.core = client.CoreV1Api()
        self.apps = client.AppsV1Api()
        self.servers = []
        if checkpoint_dir:
            self.directory = checkpoint_dir
        else:
            self.directory = "/mnt/c/Users/cetko/OneDrive/Desktop/MEng Thesis/new_faster/cs/libdpr/samples/DprCounters/DprCounters"

    def addServer(self, type:str, storage:str = None, storage_location:str = None, memory:str = None, limit:str = None):
        if type not in self.SUPPORTED_SERVER_TYPES:
            raise ArgumentError("Unsupported server type") 
        self.servers.append(self.Server(type, storage, storage_location, memory, limit))

    def isDprFinderRunning(self) -> bool:
        all_pods = self.core.list_namespaced_pod(namespace="default")
        for pod in all_pods.items:
            if pod.metadata.name == "dpr-finder-0":
                return pod.status.phase == "Running"

    def startDprFinder(self, dprPath:str = None) -> void:
        if not dprPath:
            dprPath = os.path.join(self.directory, "yaml/DprFinder.yaml")
        with open(dprPath) as f:
            dprYaml = yaml.safe_load_all(f.read())
        k = 0
        for yaml_doc in dprYaml:
            if k==0:
                self.core.create_namespaced_service("default", yaml_doc)
            elif k==1:
                self.apps.create_namespaced_stateful_set("default", yaml_doc)
            else:
                raise ArgumentError("The yaml file provided is wrongly formatted")
            k+=1

    def killDprFinder(self) -> void:
        self.core.delete_namespaced_pod(name="dpr-finder-0", namespace="default")

    def stopDprFinder(self) -> void:
        self.apps.delete_namespaced_stateful_set(name="dpr-finder", namespace = "default")
        self.core.delete_namespaced_service(name="dpr-finder-svc", namespace="default")
    
    def startCounterServers(self, counterServerService:str = None, counterServiceStateful:str = None) -> void:
        
        def attachIdService(servYaml, id):
            toAdd = "-" + str(id)
            servYaml["metadata"]["name"] += toAdd
            servYaml["metadata"]["labels"]["app"] += toAdd
            servYaml["spec"]["selector"]["app"] += toAdd
        
        def attachIdStateful(stateYaml, id):
            toAdd = "-" + str(id)
            stateYaml["metadata"]["name"] += toAdd
            stateYaml["spec"]["serviceName"] += toAdd
            stateYaml["spec"]["selector"]["matchLabels"]["app"] += toAdd
            stateYaml["spec"]["template"]["metadata"]["labels"]["app"] += toAdd
            stateYaml["spec"]["template"]["spec"]["containers"][0]["name"] += toAdd


        if not counterServerService:
            counterServerService = os.path.join(self.directory, "yaml/CounterServerService.yaml")
        if not counterServiceStateful:
            counterServiceStateful = os.path.join(self.directory, "yaml/CounterServerStateful.yaml")
        for id in range(len(self.servers)):
            server = self.servers[id] # TODO(Nikola): add up all the resource things as well
            with open(counterServerService) as f:
                serviceYaml = yaml.safe_load(f.read())
            with open(counterServiceStateful) as g:
                statefulYaml = yaml.safe_load(g.read())
            attachIdService(serviceYaml, id)
            attachIdStateful(statefulYaml, id)
            self.core.create_namespaced_service("default", serviceYaml)
            self.apps.create_namespaced_stateful_set("default", statefulYaml)

    def killCounterServer(self, id:int = 0) -> void:
        self.core.delete_namespaced_pod(name="counter-" + str(id), namespace = "default")
    
    def stopCounterServers(self) -> void:
        self.apps.delete_namespaced_stateful_set(name="counter", namespace = "default")
        self.core.delete_namespaced_service(name="counter-server-svc", namespace="default")
    
    def patchConfigMap(self) -> void:
        portPatch = dict()
        portPatch["data"] = dict()
        portPatch["data"]["6379"] = "default/dpr-finder-svc:3000"
        base_number = 6380
        base_string = "default/counter-server-svc-"
        for id in range(len(self.servers)):
            portPatch["data"][str(base_number + id)] = base_string + str(id) + ":80"
        ret = self.core.patch_namespaced_config_map("tcp-services", "ingress-nginx", portPatch)
    
    def patchIngress(self, ingressFile:str = None) -> void:
        if not ingressFile:
            ingressFile = os.path.join(self.directory, "yaml/IngressPatch.yaml")
        with open(ingressFile) as f:
            patch = yaml.safe_load(f.read())
        ports = patch["spec"]["template"]["spec"]["containers"][0]["ports"]
        basePort = 6380
        for id in range(len(self.servers)):
            ports.append({"containerPort": basePort + id, "hostPort": basePort + id})
        ret = self.apps.patch_namespaced_deployment("ingress-nginx-controller", "ingress-nginx", patch)


    def stopCluster(self) -> void:
        self.stopDprFinder()
        self.stopCounterServers()

    def start(self) -> void:
        self.startDprFinder()
        while not self.isDprFinderRunning():
            time.sleep(1)
        self.patchConfigMap()
        self.patchIngress()
        self.startCounterServers()
    
    def testing(self) -> void:
        pass


def main():
    testt = KubernetesCluster()
    testt.addServer("counter")
    testt.addServer("counter")
    testt.addServer("counter")
    testt.start()

if __name__ == "__main__":
    main()