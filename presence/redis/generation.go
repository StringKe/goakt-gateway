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

// RefreshGeneration and LeaveGeneration are the generation-fenced counterparts of Refresh
// and Leave: they let a caller that knows its connection's owner-lease generation (see the
// root gateway package's WithOwnerLease/ownerLease) attach it to a presence write, so a
// write issued by a node whose ownership a takeover has already superseded is rejected
// rather than resurrecting or destroying membership state a newer owner established since.
//
// RefreshGen and LeaveGen below adapt these onto gateway.PresenceFencer, the root package's
// name/error contract for this capability: Registry's renewPresence loop and teardown path
// type-assert the configured Presence backend up to PresenceFencer and call these instead of
// the plain, unfenced Presence.Refresh and Presence.Leave whenever it implements the interface
// (see registry.go's refreshPresence/leavePresence).
package redis

import (
	"context"
	"errors"
	"time"

	gateway "github.com/StringKe/goakt-gateway"
)

// RefreshGeneration is Refresh, additionally fenced by generation: it fails with
// ErrStaleGeneration if a later generation has already been recorded as the leave/refresh
// watermark for connID in group. See writeMemberScript for the exact fencing rule.
func (p *Presence) RefreshGeneration(ctx context.Context, group, connID string, generation uint64, ttl time.Duration) error {
	return p.writeMember(ctx, group, connID, ttl, false, nil, true, generation)
}

// LeaveGeneration is Leave, additionally fenced by generation: it fails with
// ErrStaleGeneration if a later generation has already been recorded as the leave/refresh
// watermark for connID in group, so a delayed leave from a superseded owner cannot remove a
// membership a newer owner (re)established since. See leaveScript for the exact fencing
// rule.
func (p *Presence) LeaveGeneration(ctx context.Context, group, connID string, generation uint64) error {
	return p.leaveMember(ctx, group, connID, true, generation)
}

// RefreshGen implements gateway.PresenceFencer. It is RefreshGeneration with ErrStaleGeneration
// translated to gateway.ErrStaleOwner, the sentinel PresenceFencer's contract documents.
func (p *Presence) RefreshGen(ctx context.Context, group, connID string, generation uint64, ttl time.Duration) error {
	if err := p.RefreshGeneration(ctx, group, connID, generation, ttl); err != nil {
		if errors.Is(err, ErrStaleGeneration) {
			return gateway.ErrStaleOwner
		}
		return err
	}
	return nil
}

// LeaveGen implements gateway.PresenceFencer. It is LeaveGeneration with ErrStaleGeneration
// translated to gateway.ErrStaleOwner, the sentinel PresenceFencer's contract documents.
func (p *Presence) LeaveGen(ctx context.Context, group, connID string, generation uint64) error {
	if err := p.LeaveGeneration(ctx, group, connID, generation); err != nil {
		if errors.Is(err, ErrStaleGeneration) {
			return gateway.ErrStaleOwner
		}
		return err
	}
	return nil
}

var _ gateway.PresenceFencer = (*Presence)(nil)
