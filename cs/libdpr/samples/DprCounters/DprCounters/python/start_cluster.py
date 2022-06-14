from ctypes import ArgumentError
from posixpath import basename
import resource
from kubernetes import client, config
import os
import time
from numpy import void

import yaml


class KubernetesCluster():

    DEFAULT_STORAGE = "1Gi"
    SUPPORTED_SERVER_TYPES = ["counter"]
    BASE_PORT = 6380

    class Server():

        def __init__(self, type:str, storage_location:str = None, storage_capacity:str = None, cpu_request:str = None, cpu_limit:str = None, memory_request:str = None, memory_limit:str = None):
            self.type = type
            self.storage_location = storage_location
            self.storage_capacity = storage_capacity
            self.cpu_request = cpu_request
            self.cpu_limit = cpu_limit
            self.memory_request = memory_request
            self.memory_limit = memory_limit

    def __init__(self, checkpoint_dir:str = None) -> void:
        config.load_kube_config()
        self.core = client.CoreV1Api()
        self.apps = client.AppsV1Api()
        self.servers = []
        if checkpoint_dir:
            self.directory = checkpoint_dir
        else:
            self.directory = "/mnt/c/Users/cetko/OneDrive/Desktop/MEng Thesis/new_faster/cs/libdpr/samples/DprCounters/DprCounters"

    def addServer(self, type:str, storage_location:str = None, storage_capacity:str = None, cpu_request:str = None, cpu_limit:str = None, memory_request:str = None, memory_limit:str = None):
        if type not in self.SUPPORTED_SERVER_TYPES:
            raise ArgumentError("Unsupported server type") 
        self.servers.append(self.Server(type, storage_location, storage_capacity, cpu_request, cpu_limit, memory_request, memory_limit))

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
        
        def addPortEnvironmentVar(stateYaml, port):
            addTemplate = {'name': 'FRONTEND_PORT', 'value': str(port)}
            env = stateYaml["spec"]["template"]["spec"]["containers"][0]["env"]
            env.append(addTemplate)
        
        def specifyStorage(stateYaml, server):
            if server.storage_location:
                stateYaml["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][0]["mountPath"] = server.storage_location

        def specifyStorageCapacity(stateYaml, server):
            if server.storage_capacity:
                stateYaml["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"] = server.storage_capacity

        def specifyCpuRequest(stateYaml, server):
            if server.cpu_request:
                resource_path = stateYaml["spec"]["template"]["spec"]["containers"][0]
                resource_path["resources"] = dict()
                resource_path["resources"]["requests"] = dict()
                resource_path["resources"]["requests"]["cpu"] = server.cpu_request

        def specifyCpuLimit(stateYaml, server):
            if server.cpu_limit:
                resource_path = stateYaml["spec"]["template"]["spec"]["containers"][0]
                resource_path["resources"] = dict()
                resource_path["resources"]["limits"] = dict()
                resource_path["resources"]["limits"]["cpu"] = server.cpu_limit

        def specifyMemoryRequest(stateYaml, server):
            if server.memory_request:
                resource_path = stateYaml["spec"]["template"]["spec"]["containers"][0]
                resource_path["resources"] = dict()
                resource_path["resources"]["requests"] = dict()
                resource_path["resources"]["requests"]["memory"] = server.memory_request

        def specifyMemoryLimit(stateYaml, server):
            if server.memory_limit:
                resource_path = stateYaml["spec"]["template"]["spec"]["containers"][0]
                resource_path["resources"] = dict()
                resource_path["resources"]["limits"] = dict()
                resource_path["resources"]["limits"]["memory"] = server.memory_limit


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
            addPortEnvironmentVar(statefulYaml, self.BASE_PORT + id)
            specifyStorage(statefulYaml, server)
            specifyStorageCapacity(statefulYaml, server)
            specifyCpuRequest(statefulYaml, server)
            specifyCpuLimit(statefulYaml, server)
            specifyMemoryRequest(statefulYaml, server)
            specifyMemoryLimit(statefulYaml, server)
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
        base_string = "default/counter-server-svc-"
        for id in range(len(self.servers)):
            portPatch["data"][str(self.BASE_PORT + id)] = base_string + str(id) + ":80"
        ret = self.core.patch_namespaced_config_map("tcp-services", "ingress-nginx", portPatch)
    
    def patchIngress(self, ingressFile:str = None) -> void:
        if not ingressFile:
            ingressFile = os.path.join(self.directory, "yaml/IngressPatch.yaml")
        with open(ingressFile) as f:
            patch = yaml.safe_load(f.read())
        ports = patch["spec"]["template"]["spec"]["containers"][0]["ports"]
        for id in range(len(self.servers)):
            ports.append({"containerPort": self.BASE_PORT + id, "hostPort": self.BASE_PORT + id})
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
    cluster = KubernetesCluster()
    cluster.addServer("counter", cpu_request="100Mi", cpu_limit="1Gi", memory_request = "100Mi", memory_limit = "1Gi")
    cluster.addServer("counter")
    cluster.start()

if __name__ == "__main__":
    main()