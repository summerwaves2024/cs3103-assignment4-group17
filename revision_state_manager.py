# Revision State Manager holds ownership over segments states.
# - Decides whether to reject or accept updates etc
# - Should handle provisional-to-final transitions
# - Identifies out-of-order packets
#
# Deciding which packet to resend is someone else's task

from data_structs import Caption

class RevisionStateManager:
    def __init__(self, session : str):
        self._session = session
        self._segments: dict[int, Caption] = {}

        # The "earliest" segment that still hasn't been finalized
        self._latest_active_seg : int = 1

    # Updates segments if caption is accepted (e.g. revision_no not outdated)
    # out: (is_accepted, is_seg_out_of_order)
    def process_caption(self, c : Caption) -> tuple[bool, bool]:

        # Reject malformed captions (missing / wrongly typed fields)
        if not isinstance(c.segment, int) or not isinstance(c.revision, int):
            return False, False

        # Reject if wrong session
        if c.session != self._session:
            return False, False
                
        cap_seg_id : int = c.segment

        # caption_source has no segment id < 1, marking this case as invalid
        if cap_seg_id < 1:
            return False, False

        # If this Caption is entirely new, add it to segments
        if cap_seg_id not in self._segments:
            is_out_of_order = self.add_to_segments(c, cap_seg_id)
            return True, is_out_of_order

        curr_seg : Caption = self._segments[cap_seg_id]
        revision_diff : int = c.revision - curr_seg.revision 

        # Discard outdated / duplicate revisions
        if revision_diff <= 0 or curr_seg.final:
            return False, False

        # Remainder are all valid segments (not necessarily in order)
        is_out_of_order = self.add_to_segments(c, cap_seg_id)
                
        return True, is_out_of_order

    def get_segments(self) -> dict[int, Caption]:
        return self._segments

    def get_segment(self, segment_id : int) -> Caption | None:
        return self._segments.get(segment_id)

    def add_to_segments(self, c : Caption, seg_id : int) -> bool:
        self._segments[seg_id] = c

        is_wrong_order = False

        # If the segment id is beyond the earliest non-final
        # segment, it's out of order (but we still accept it)
        if seg_id > self._latest_active_seg:
            is_wrong_order = True

        # When a final segment is accepted, we check if we need
        # to adjust the active segment. (Note: an out of order segment
        # segment may be final and accepted, hence the loop from active)
        elif c.final:
            i = self._latest_active_seg
            while i in self._segments:
                if self._segments[i].final:
                    self._latest_active_seg += 1
                i += 1
        
        return is_wrong_order
