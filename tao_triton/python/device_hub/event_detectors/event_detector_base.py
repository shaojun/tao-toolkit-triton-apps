from tao_triton.python.device_hub.board_timeline import BoardTimeline


class EventDetectorBase:
    def __init__(self, logging):
        self.event_listeners = []
        self.state_obj: dict = None
        self.timeline: BoardTimeline = None
        self.is_session_detector = False

    def prepare(self, timeline, event_detectors):
        """
        before call the `detect`, this function is guaranteed to be called ONLY once.
        @param timeline: BoardTimeline
        @type event_detectors: List[EventDetectorBase]
        @param event_detectors: other detectors in pipeline, could be used for subscribe inner events.
        """
        self.timeline = timeline
        pass

    def get_timeline_item_filter(self):
        def filter(timeline_items):
            """

            @param timeline_items: List[TimelineItem]
            @return:
            """
            return None

        return None

    def subscribe_on_property_changed(self, event_listener):
        """
        registering the caller as a subscriber to listen the inner property changed event
        @param event_listener: the caller must has the function of on_property_changed_event_handler(..., ...).
        """
        self.event_listeners.append(event_listener)

    def on_property_changed_event_handler(self, src_detector, property_name: str, data):
        """
        will be called if the inner property changed event get fired
        @param src_detector: the detector who firing the inner property changed event
        @param property_name: the name of the property has the data changed on
        @param data: the data with the event
        """
        pass

    def __fire_on_property_changed_event_to_subscribers__(self, property_name: str, data):
        """
        fire an inner property changed event to all subscribers
        @param property_name: the name of the property has the data changed on
        @param data: the data with the event
        """
        if self.event_listeners:
            for el in self.event_listeners:
                el.on_property_changed_event_handler(self, property_name, data)

    def detect(self, filtered_timeline_items):
        """

        @param filtered_timeline_items: List[TimelineItem]
        @return: List[EventAlarm]
        """
        return None

    # def where(self, items: List[TimelineItem], last_count: int):
    #     return items[-last_count:]
