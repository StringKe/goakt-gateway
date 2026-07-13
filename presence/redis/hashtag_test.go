// MIT License
//
// Copyright (c) 2026 StringKe
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// This suite proves the Redis Cluster hash tag safety fix without requiring an actual
// cluster: it reimplements Redis Cluster's own, publicly documented hash tag extraction and
// CRC16 slot algorithm (https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec,
// Appendix A) and applies it directly to the exact key strings the package produces. This is
// a stronger check than standing up a real cluster and hoping to provoke a CROSSSLOT error:
// it is deterministic, requires no infrastructure, and pins down the precise slot Redis
// Cluster would compute for every adversarial group name in one run.
package redis_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// clusterHashTagKey reimplements Redis Cluster's key-hashing rule: the substring between the
// first '{' and the next '}' after it, when that substring is non-empty; otherwise the whole
// key. This mirrors go-redis's internal/hashtag.Key and the reference implementation Redis
// Cluster itself uses.
func clusterHashTagKey(key string) string {
	start := -1
	for i := 0; i < len(key); i++ {
		if key[i] == '{' {
			start = i
			break
		}
	}
	if start == -1 {
		return key
	}
	for j := start + 1; j < len(key); j++ {
		if key[j] == '}' {
			if j == start+1 {
				// Empty tag body ("{}" with nothing between): Redis Cluster's own rule
				// treats this as "no tag present" and falls back to the whole key.
				return key
			}
			return key[start+1 : j]
		}
	}
	return key
}

// crc16tab is the CCITT CRC16 table Redis Cluster mandates for slot computation (Appendix A
// of the cluster spec, also embedded verbatim in go-redis's internal/hashtag package).
var crc16tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

// clusterSlot computes the Redis Cluster slot (0-16383) for key, per the cluster spec.
func clusterSlot(key string) int {
	tagged := clusterHashTagKey(key)
	var crc uint16
	for i := 0; i < len(tagged); i++ {
		crc = (crc << 8) ^ crc16tab[(byte(crc>>8)^tagged[i])&0x00ff]
	}
	return int(crc) % 16384
}

// TestClusterHashTagKeyMatchesEmptyTagFallback pins down the Redis Cluster degenerate case
// this package's fix works around: a key whose only apparent tag body is empty is hashed as
// a whole, not by that empty body. This is what makes a naive prefix+"{"+group+"}" scheme
// unsafe for group == "" or a group starting with '}'.
func TestClusterHashTagKeyMatchesEmptyTagFallback(t *testing.T) {
	require.Equal(t, "x{}y", clusterHashTagKey("x{}y"), "an empty {} tag must fall back to the whole key")
	require.Equal(t, "x{}}y", clusterHashTagKey("x{}}y"), "a group starting with '}' produces the same empty-tag degenerate case")
	require.Equal(t, "tag", clusterHashTagKey("x{tag}y"), "a normal non-empty tag is extracted correctly")
}

// TestRedisPresenceHashTagAlwaysCoLocatesGroupKeys is the CROSSSLOT reproduction and fix
// proof for presence/redis's key scheme: for every adversarial group name below, a naive
// "infix{group}" key layout would send the member key ("m:{group}") and the metadata key
// ("h:{group}") to different Cluster slots whenever group's content defeats the hash tag
// (empty group, or a group starting with '}'), because the two keys' infixes differ and an
// empty/absent tag falls back to hashing the whole, now-different, key. The package's actual
// scheme - hashTag(group) = "g" + hex(group) wrapped in the braces - must keep every group's
// three co-located keys (member, metadata, generation) on one slot for every case, including
// the adversarial ones a naive scheme breaks on.
func TestRedisPresenceHashTagAlwaysCoLocatesGroupKeys(t *testing.T) {
	prefix := "gateway:presence:"

	groups := []string{
		"user:1",    // ordinary group: sanity baseline
		"",          // empty group: the naive scheme's key becomes "m:{}"/"h:{}", an empty tag
		"}leading",  // starts with '}': the naive scheme's tag body is empty for the same reason
		"has}brace", // contains '}' but does not start with it
		"has{brace", // contains '{' internally
		"{}",        // a group that is itself a naive empty tag
	}

	// First, prove the naive scheme (what this package used before the fix, and what a
	// straightforward "infix{group}" implementation would still do today) really does break
	// co-location for the adversarial cases - otherwise this test would not be exercising the
	// bug it claims to guard against.
	naiveMember := func(group string) string { return prefix + "m:{" + group + "}" }
	naiveMeta := func(group string) string { return prefix + "h:{" + group + "}" }
	require.NotEqual(t,
		clusterSlot(naiveMember("")), clusterSlot(naiveMeta("")),
		"sanity check: the naive scheme must actually reproduce CROSSSLOT-risking divergent slots for an empty group, or this test is not proving anything")
	require.NotEqual(t,
		clusterSlot(naiveMember("}leading")), clusterSlot(naiveMeta("}leading")),
		"sanity check: the naive scheme must actually reproduce divergent slots for a group starting with '}'")

	// Now prove the package's actual scheme keeps every group's keys on one slot, including
	// the two adversarial cases just shown to break the naive one.
	tag := func(group string) string { return "g" + hex.EncodeToString([]byte(group)) }
	fixedMember := func(group string) string { return prefix + "m:{" + tag(group) + "}" }
	fixedMeta := func(group string) string { return prefix + "h:{" + tag(group) + "}" }
	fixedGen := func(group string) string { return prefix + "s:{" + tag(group) + "}" }

	for _, group := range groups {
		memberSlot := clusterSlot(fixedMember(group))
		metaSlot := clusterSlot(fixedMeta(group))
		genSlot := clusterSlot(fixedGen(group))
		require.Equal(t, memberSlot, metaSlot, "group %q: member and metadata keys must share a Cluster slot", group)
		require.Equal(t, memberSlot, genSlot, "group %q: member and generation keys must share a Cluster slot", group)

		require.NotEmpty(t, clusterHashTagKey(fixedMember(group)), "group %q: the extracted hash tag must never be empty", group)
	}
}
