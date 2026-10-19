# Revision State Manager holds ownership over segments states.
# - Decides whether to reject or accept updates etc
# - Should handle provisional-to-final transitions
# - Identifies out-of-order packets
#
# Deciding which packet to resend is someone else's task

from data_structs import Caption

class RevisionStateManager:
    def __init__(self):
        self._segments: dict[int, Caption] = {}

    # out: (is_accepted, is_out_of_order)
    def update_caption(c : Caption) -> tuple[bool, bool]:
        return True, True

    def get_segments(self) -> dict[int, Caption]:
        return self._segments

    def get_segment(self, segment_id : int) -> Caption:
            return self._segments.get(segment_id)

    