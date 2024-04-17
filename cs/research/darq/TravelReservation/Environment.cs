using Azure.Storage.Blobs;
using FASTER.core;
using FASTER.libdpr;

namespace TravelReservation;

public interface IEnvironment
{
    public string GetOrchestratorConnString();

    public int GetOrchestratorPort(Options options);

    public DeviceLogCommitCheckpointManager GetOrchestratorCheckpointManager(Options options);

    public IDevice GetOrchestratorDevice(Options options);

    public string GetServiceConnString(int index);

    public int GetServicePort(Options options);

    public DeviceLogCommitCheckpointManager GetServiceCheckpointManager(Options options);

    public IDevice GetServiceDevice(Options options);

    public string GetDprFinderConnString();

    public int GetDprFinderPort();

    public PingPongDevice GetDprFinderDevice();

    public Task PublishResultsAsync(string fileName, MemoryStream bytes);
}

public class LocalDebugEnvironment : IEnvironment
{
    private string connString = "";

    public string GetOrchestratorConnString() => "http://127.0.0.1:15721";

    public int GetOrchestratorPort(Options options)
    {
        if (options.WorkerName != 1) throw new NotImplementedException();
        return 15721;
    }

    public DeviceLogCommitCheckpointManager GetOrchestratorCheckpointManager(Options options)
    {
        var result = new DeviceLogCommitCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\orchestrators{options.WorkerName}"), removeOutdated: false);
        result.PurgeAll();
        return result;
    }

    public IDevice GetOrchestratorDevice(Options options) =>
        new LocalStorageDevice($"D:\\orchestator{options.WorkerName}.log", deleteOnClose: true);

    public string GetServiceConnString(int index)
    {
        if (index != 0) throw new NotImplementedException();
        return "http://127.0.0.1:15722";
    }

    public int GetServicePort(Options options)
    {
        if (options.WorkerName != 0) throw new NotImplementedException();
        return 15722;
    }

    public DeviceLogCommitCheckpointManager GetServiceCheckpointManager(Options options)
    {
        var result = new DeviceLogCommitCheckpointManager(
            new LocalStorageNamedDeviceFactory(),
            new DefaultCheckpointNamingScheme($"D:\\service{options.WorkerName}"), removeOutdated: false);
        result.PurgeAll();
        return result;
    }

    public IDevice GetServiceDevice(Options options) =>
        new LocalStorageDevice($"D:\\service{options.WorkerName}.log", deleteOnClose: true);

    public string GetDprFinderConnString() => "http://127.0.0.1:15720";

    public int GetDprFinderPort() => 15720;

    public PingPongDevice GetDprFinderDevice()
    {
        var device1 = new LocalMemoryDevice(1 << 24, 1 << 24, 1);
        var device2 = new LocalMemoryDevice(1 << 24, 1 << 24, 1);
        return new PingPongDevice(device1, device2, true);
    }

    public Task PublishResultsAsync(string fileName, MemoryStream bytes)
    {
        Console.WriteLine($"Results for {fileName}:");
        var reader = new StreamReader(bytes);
        var text = reader.ReadToEnd();
        // Print to console
        Console.Write(text);
        return Task.CompletedTask;
    }
}

public class KubernetesNoFailureLocalStorageEnvironment : IEnvironment
{
    public string GetOrchestratorConnString() => "http://orchestrator.dse.svc.cluster.local:15721";

    public int GetOrchestratorPort(Options options) => 15721;

    public DeviceLogCommitCheckpointManager GetOrchestratorCheckpointManager(Options options)
    {
        // TODO(Tianyu): Figure out how to mount storage volumes on Kubernetes 
        throw new NotImplementedException();
    }

    public IDevice GetOrchestratorDevice(Options options)
    {
        // TODO(Tianyu): Figure out how to mount storage volumes on Kubernetes 
        throw new NotImplementedException();
    }

    public string GetServiceConnString(int index) => $"http://service{index}.dse.svc.cluster.local:15721";

    public int GetServicePort(Options options) => 15721;

    public DeviceLogCommitCheckpointManager GetServiceCheckpointManager(Options options)
    {
        // TODO(Tianyu): Figure out how to mount storage volumes on Kubernetes 
        throw new NotImplementedException();
    }

    public IDevice GetServiceDevice(Options options)
    {
        // TODO(Tianyu): Figure out how to mount storage volumes on Kubernetes 
        throw new NotImplementedException();
    }

    public string GetDprFinderConnString() => "http://dprfinder.dse.svc.cluster.local:15721";

    public int GetDprFinderPort() => 15721;

    public PingPongDevice GetDprFinderDevice()
    {
        // TODO(Tianyu): Figure out how to mount storage volumes on Kubernetes 
        throw new NotImplementedException();
    }

    public async Task PublishResultsAsync(string fileName, MemoryStream bytes)
    {
        var connString = Environment.GetEnvironmentVariable("AZURE_RESULTS_CONN_STRING");
        var blobServiceClient = new BlobServiceClient(connString);
        var blobContainerClient = blobServiceClient.GetBlobContainerClient("results");
        
        await blobContainerClient.CreateIfNotExistsAsync();
        var blobClient = blobContainerClient.GetBlobClient(fileName);
        
        await blobClient.UploadAsync(bytes, overwrite: true);    
    }
    
}