package io

import (
	"math"

	"github.com/nickpoorman/arrow-parquet-go/internal/debug"
)

const kDefaultIdealBandwidthUtilizationFrac float64 = 0.9
const kDefaultMaxIdealRequestSizeMib int64 = 64

type CacheOptions struct {
	// The maximum distance in bytes between two consecutive
	//   ranges; beyond this value, ranges are not combined
	holeSizeLimit int64

	// The maximum size in bytes of a combined range; if
	//   combining two consecutive ranges would produce a range of a
	//   size greater than this, they are not combined
	rangeSizeLimit int64
}

func (c *CacheOptions) Equals(other *CacheOptions) bool {
	return c.holeSizeLimit == other.holeSizeLimit &&
		c.rangeSizeLimit == other.rangeSizeLimit
}

func NewCacheOptionsDefaults() *CacheOptions {
	return &CacheOptions{
		ReadRangeCache_kDefaultHoleSizeLimit,
		ReadRangeCache_kDefaultRangeSizeLimit,
	}
}

// Construct CacheOptions from network storage metrics (e.g. S3).
//
// timeToFirstByteMillis Seek-time or Time-To-First-Byte (TTFB) in
//   milliseconds, also called call setup latency of a new S3 request.
//   The value is a positive integer.
// transferBandwidthMibPerSec Data transfer Bandwidth (BW) in MiB/sec.
//   The value is a positive integer.
// idealBandwidthUtilizationFrac Transfer bandwidth utilization fraction
//   (per connection) to maximize the net data load.
//   The value is a positive double precision number less than 1.
// maxIdealRequestSizeMib The maximum single data request size (in MiB)
//   to maximize the net data load.
//   The value is a positive integer.
// return A new instance of CacheOptions.
func NewCacheOptionsFromNetworkMetrics(
	timeToFirstByteMillis int64,
	transferBandwidthMibPerSec int64,
	idealBandwidthUtilizationFrac float64,
	maxIdealRequestSizeMib int64,
) *CacheOptions {
	if idealBandwidthUtilizationFrac == -1 {
		idealBandwidthUtilizationFrac = kDefaultIdealBandwidthUtilizationFrac
	}
	if maxIdealRequestSizeMib == -1 {
		maxIdealRequestSizeMib = kDefaultMaxIdealRequestSizeMib
	}

	//
	// The I/O coalescing algorithm uses two parameters:
	//   1. holeSizeLimit (a.k.a max_io_gap): Max I/O gap/hole size in bytes
	//   2. rangeSizeLimit (a.k.a ideal_request_size): Ideal I/O Request size in bytes
	//
	// These parameters can be derived from network metrics (e.g. S3) as described below:
	//
	// In an S3 compatible storage, there are two main metrics:
	//   1. Seek-time or Time-To-First-Byte (TTFB) in seconds: call setup latency of a new
	//      S3 request
	//   2. Transfer Bandwidth (BW) for data in bytes/sec
	//
	// 1. Computing holeSizeLimit:
	//
	//   holeSizeLimit = TTFB * BW
	//
	//   This is also called Bandwidth-Delay-Product (BDP).
	//   Two byte ranges that have a gap can still be mapped to the same read
	//   if the gap is less than the bandwidth-delay product [TTFB * TransferBandwidth],
	//   i.e. if the Time-To-First-Byte (or call setup latency of a new S3 request) is
	//   expected to be greater than just reading and discarding the extra bytes on an
	//   existing HTTP request.
	//
	// 2. Computing rangeSizeLimit:
	//
	//   We want to have high bandwidth utilization per S3 connections,
	//   i.e. transfer large amounts of data to amortize the seek overhead.
	//   But, we also want to leverage parallelism by slicing very large IO chunks.
	//   We define two more config parameters with suggested default values to control
	//   the slice size and seek to balance the two effects with the goal of maximizing
	//   net data load performance.
	//
	//   BW_util_frac (ideal bandwidth utilization): Transfer bandwidth utilization fraction
	//     (per connection) to maximize the net data load. 90% is a good default number for
	//     an effective transfer bandwidth.
	//
	//   MAX_IDEAL_REQUEST_SIZE: The maximum single data request size (in MiB) to maximize
	//     the net data load. 64 MiB is a good default number for the ideal request size.
	//
	//   The amount of data that needs to be transferred in a single S3 get_object
	//   request to achieve effective bandwidth eff_BW = BW_util_frac * BW is as follows:
	//     eff_BW = rangeSizeLimit / (TTFB + rangeSizeLimit / BW)
	//
	//   Substituting TTFB = holeSizeLimit / BW and eff_BW = BW_util_frac * BW, we get the
	//   following result:
	//     rangeSizeLimit = holeSizeLimit * BW_util_frac / (1 - BW_util_frac)
	//
	//   Applying the MAX_IDEAL_REQUEST_SIZE, we get the following:
	//     rangeSizeLimit = min(MAX_IDEAL_REQUEST_SIZE,
	//                            holeSizeLimit * BW_util_frac / (1 - BW_util_frac))
	//

	debug.AssertGT(timeToFirstByteMillis, 0, "TTFB must be > 0")
	debug.AssertGT(transferBandwidthMibPerSec, 0, "Transfer bandwidth must be > 0")
	debug.AssertGT(idealBandwidthUtilizationFrac, 0,
		"Ideal bandwidth utilization fraction must be > 0")
	debug.AssertLT(idealBandwidthUtilizationFrac, 1.0,
		"Ideal bandwidth utilization fraction must be < 1")
	debug.AssertGT(maxIdealRequestSizeMib, 0, "Max Ideal request size must be > 0")

	timeToFirstByteSec := float64(timeToFirstByteMillis / 1000.0)
	transferBandwidthBytesPerSec := transferBandwidthMibPerSec * 1024 * 1024
	maxIdealRequestSizeBytes := maxIdealRequestSizeMib * 1024 * 1024

	// holeSizeLimit = TTFB * BW
	holeSizeLimit := math.Round(
		timeToFirstByteSec * float64(transferBandwidthBytesPerSec),
	)
	debug.AssertGT(holeSizeLimit, 0, "Computed holeSizeLimit must be > 0")

	// rangeSizeLimit = min(MAX_IDEAL_REQUEST_SIZE,
	//                        holeSizeLimit * BW_util_frac / (1 - BW_util_frac))
	rangeSizeLimit := int64(math.Round(
		holeSizeLimit * idealBandwidthUtilizationFrac / (1 - idealBandwidthUtilizationFrac),
	))
	if maxIdealRequestSizeBytes < rangeSizeLimit {
		rangeSizeLimit = maxIdealRequestSizeBytes
	}
	debug.AssertGT(rangeSizeLimit, 0, "Computed rangeSizeLimit must be > 0")

	return &CacheOptions{
		holeSizeLimit:  int64(holeSizeLimit),
		rangeSizeLimit: rangeSizeLimit,
	}
}

const ReadRangeCache_kDefaultHoleSizeLimit int64 = 8192
const ReadRangeCache_kDefaultRangeSizeLimit int64 = 32 * 1024 * 1024
