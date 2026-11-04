# Tracks the newest accepted revision for each caption segment.
# It validates session and revision ordering.
# It handles provisional-to-final transitions and rejects stale or duplicate updates.
# Delivery and recovery
# are handled by the server rather than by this class.

from data_structs import Caption

class RevisionStateManager:
    def __init__(self, session : str):
        self._session = session
        self._segments: dict[int, Caption] = {}

        # Earliest segment that has not yet been finalised.
        self._latest_active_seg : int = 1

        # Highest segment removed by prune(). Older segments are outside the
        # active history window and must not re-enter as new captions.
        self._pruned_upto : int = 0

    # Accept a caption only when it is newer than the stored segment state.
    # Return whether the caption was accepted and whether it arrived out of order.
    def process_caption(self, c : Caption) -> tuple[bool, bool]:

        # Reject missing or incorrectly typed fields.
        if not isinstance(c.segment, int) or not isinstance(c.revision, int):
            return False, False

        # Reject updates belonging to another session.
        if c.session != self._session:
            return False, False
                
        cap_seg_id : int = c.segment

        # Segment identifiers are one-based.
        if cap_seg_id < 1:
            return False, False

        # This segment has already been pruned from the recovery window.
        if cap_seg_id <= self._pruned_upto:
            return False, False

        # First update for this segment: store it as the current state.
        if cap_seg_id not in self._segments:
            is_out_of_order = self.add_to_segments(c, cap_seg_id)
            return True, is_out_of_order

        curr_seg : Caption = self._segments[cap_seg_id]
        revision_diff : int = c.revision - curr_seg.revision 

        # Ignore duplicate or older revisions.
        if revision_diff <= 0 or curr_seg.final:
            return False, False

        # The update is newer and can be accepted even if segments arrive
        # out of order.
        is_out_of_order = self.add_to_segments(c, cap_seg_id)
                
        return True, is_out_of_order

    # Remove segments at or below seg_id so the manager remains bounded.
    # The server calls this when finalised captions leave recovery history.
    def prune(self, seg_id : int) -> None:
        if seg_id <= self._pruned_upto:
            return
        pruned_segments = [s for s in self._segments if s <= seg_id]
        for s in pruned_segments:
            del self._segments[s]
        self._pruned_upto = seg_id
        self._latest_active_seg = max(self._latest_active_seg, seg_id + 1)

    def get_segments(self) -> dict[int, Caption]:
        return self._segments

    def get_segment(self, segment_id : int) -> Caption | None:
        return self._segments.get(segment_id)

    def add_to_segments(self, c : Caption, seg_id : int) -> bool:
        self._segments[seg_id] = c

        is_wrong_order = False

        # A segment beyond the earliest active segment arrived out of order;
        # this is valid but worth reporting to the caller.
        if seg_id > self._latest_active_seg:
            is_wrong_order = True

        # When a segment becomes final advance the active-segment marker when needed.
        # Final segments may arrive out of order so the loop is required.
        elif c.final:
            i = self._latest_active_seg
            while i in self._segments:
                if self._segments[i].final:
                    self._latest_active_seg += 1
                i += 1
        
        return is_wrong_order
