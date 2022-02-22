using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace FASTER.libdpr
{
    public class ConfigurationResponse
    {
        /// <summary>
        ///     end offset of the serialized portion of cluster state on the response buffer
        /// </summary>
        public int recoveryStateEnd;

        /// <summary>
        ///     end offset of the entire response on the buffer
        /// </summary>
        public int responseEnd;

        /// <summary>
        ///     Reader/Writer latch that protects members --- if reading the serialized response, must do so under
        ///     read latch to prevent concurrent modification.
        /// </summary>
        public ReaderWriterLockSlim rwLatch;

        /// <summary>
        ///     buffer that holds the serialized state
        /// </summary>
        public byte[] serializedResponse = new byte[1 << 15];

        /// <summary>
        ///     Create a new PrecomputedSyncResponse from the given cluster state. Until UpdateCut is called, the
        ///     serialized response will have a special value for the cut to signal its absence.
        /// </summary>
        /// <param name="clusterState"> cluster state to serialize </param>
        public ConfigurationResponse(Dictionary<Worker, IPEndPoint> workers)
        {
            rwLatch = new ReaderWriterLockSlim();
            // Reserve space for dict_size + actual_dict fields
            var serializedSize = RespUtil.DictionarySerializedSize(workers);
            // Resize response buffer to fit
            if (serializedSize > serializedResponse.Length)
                serializedResponse = new byte[Math.Max(2 * serializedResponse.Length, serializedSize)];

            recoveryStateEnd =
                RespUtil.SerializeDictionary(workers, serializedResponse, 0);
            // recoveryStateEnd should point to the very end, while the serializedResponse should contain everything I need
            responseEnd = recoveryStateEnd;
        }

        public static Dictionary<Worker, IPEndPoint> FromBuffer(byte[] buf, int offset, out int head)
        {
            var result = new Dictionary<Worker, IPEndPoint>();
            head = RespUtil.ReadDictionaryFromBytes(buf, offset, result);
            return result;
        }
    }

    public class ClusterBackend
    {
        private Dictionary<Worker, IPEndPoint> clusterInfo;
        private ConfigurationResponse response;
        public ClusterBackend(Dictionary<Worker, IPEndPoint> clusterInfo)
        {
            // passing by reference, so updating inside cluster should update the backend whenever we refresh?
            // is that good practice?
            this.clusterInfo = clusterInfo;
            this.response = new ConfigurationResponse(this.clusterInfo);
        }

        public void Refresh()
        {
            this.response = new ConfigurationResponse(this.clusterInfo);
        }
        public ConfigurationResponse getClusterState()
        {
            return response;
        }
    }
}
