﻿using darq;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.darq
{
    
    /// <summary>
    ///     Each DARQ instance is uniquely numbered within a cluster  for identification and routing
    /// </summary>
    public struct DarqId : IEquatable<DarqId>
    {

        /// <summary>
        ///  globally-unique worker ID within a DPR cluster
        /// </summary>
        public readonly long guid;

        /// <summary>
        ///     Constructs a worker with the given guid
        /// </summary>
        /// <param name="guid"> worker guid </param>
        public DarqId(long guid)
        {
            this.guid = guid;
        }

        public readonly bool Equals(DarqId other)
        {
            return guid == other.guid;
        }

        public static bool operator ==(DarqId left, DarqId right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(DarqId left, DarqId right)
        {
            return !left.Equals(right);
        }

        /// <inheritdoc cref="object" />
        public override bool Equals(object obj)
        {
            return obj is DarqId other && Equals(other);
        }

        /// <inheritdoc cref="object" />
        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }
    }
    
    /// <summary>
    /// DARQ Settings
    /// </summary>
    public class DarqSettings
    {
        /// <summary>
        ///  The DPRFinder for the cluster this DARQ should connect to, or null if you will only use DARQ
        ///  non-speculatively and do not wish to connect to a DPR cluster. If a non-null DPRFinder is supplied,
        ///  DARQ will operate in speculative mode.
        /// </summary>
        public IDprFinder DprFinder = null;
        
        public DarqId Me = new(0);

        public DprWorkerId MyDpr = DprWorkerId.INVALID;
        
        public long CheckpointPeriodMilli = 5;
        
        public long RefreshPeriodMilli = 5;
        
        /// <summary>
        /// Device used for underlying log
        /// </summary>
        public IDevice LogDevice;
        

        /// <summary>
        /// Size of a page in the underlying log, in bytes. Must be a power of 2.
        /// </summary>
        public long PageSize = 1L << 22;
        
        /// <summary>
        /// Total size of in-memory part of log, in bytes. Must be a power of 2.
        /// Should be at least one page long
        /// Num pages = 2^(MemorySizeBits-PageSizeBits)
        /// </summary>
        public long MemorySize = 1L << 23;
        

        /// <summary>
        /// Size of a segment (group of pages), in bytes. Must be a power of 2.
        /// This is the granularity of files on disk
        /// </summary>
        public long SegmentSize = 1L << 30;

        /// <summary>
        /// Log commit manager - if you want to override the default implementation of commit.
        /// </summary>
        public ILogCommitManager LogCommitManager = null;

        /// <summary>
        /// Use specified directory (path) as base for storing and retrieving underlying log commits. By default,
        /// commits will be stored in a folder named log-commits under this directory. If not provided, 
        /// we use the base path of the log device by default.
        /// </summary>
        public string LogCommitDir = null;
        
        /// <summary>
        /// Type of checksum to add to log
        /// </summary>
        public LogChecksumType LogChecksum = LogChecksumType.None;

        /// <summary>
        /// Fraction of underlying log marked as mutable (uncommitted)
        /// </summary>
        public double MutableFraction = 0;
        
        /// <summary>
        /// When FastCommitMode is enabled, FasterLog will reduce commit critical path latency, but may result in slower
        /// recovery to a commit on restart. Additionally, FastCommitMode is only possible when log checksum is turned
        /// on.
        /// </summary>
        public bool FastCommitMode = false;

        /// <summary>
        /// When DeleteOnClose is true, DARQ will remove all persistent state on shutdown -- useful for testing but
        /// will result in data loss otherwise,
        /// </summary>
        public bool DeleteOnClose = false;
        
        /// <summary>
        /// When CleanStart is true, DARQ will remove all persistent previous state on startup -- useful for testing but
        /// will result in data loss otherwise
        /// </summary>
        public bool CleanStart = false;

        /// <summary>
        /// Create default configuration settings for DARQ. You need to create and specify LogDevice 
        /// explicitly with this API.
        /// Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB").
        /// </summary>
        public DarqSettings() { }

        /// <inheritdoc />
        public override string ToString()
        {
            var retStr = $"log memory: {Utility.PrettySize(MemorySize)}; log page: {Utility.PrettySize(PageSize)}; log segment: {Utility.PrettySize(SegmentSize)}";
            retStr += $"; log device: {(LogDevice == null ? "null" : LogDevice.GetType().Name)}";
            retStr += $"; mutable fraction: {MutableFraction}; fast commit mode: {(FastCommitMode ? "yes" : "no")}";
            retStr += $"; delete on close: {(DeleteOnClose ? "yes" : "no")}";
            return retStr;
        }
    }
}