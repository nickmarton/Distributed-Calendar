"""Node class for Distributed Systems Project 1."""

from Event import Event
from Appointment import Appointment

class Node(object):
    """
    Node object.

    node_count:     number of total Nodes in the distributed system enforced as
                    an integer; node_id must be strictly less than this. 
    node_id:        unique id of this Node; the creator of the Node is
                    responsible for ensuring the id is unique.
    clock:          local clock of the Node enforced as an integer and
                    incremented whenever it is referenced.
    calendar:       local calendar of events maintained as a dictionary
                    data structure by this Node.
    log:            local log of event records maintained by this Node.
    T:              this Node's 2D Time Table.
    """

    def __init__(self, node_id, node_count):
        """Initialize a new Node object."""
        if not isinstance(node_id, int):
            raise TypeError("node_id parameter must be of type int.")
        if not isinstance(node_count, int):
            raise TypeError("node_count parameter must be of type int.")
        
        if node_id > node_count:
            raise ValueError(
                "node_id can not exceed total number of Nodes (node_count)")

        self._id = node_id
        self._clock = 0
        self._calendar = {}
        self._log = []
        self._T = [[0 for j in range(node_count)] for i in range(node_count)]

    def _is_conflicting(self, X):
        """
        Determine if the provided appointment is conflicting with an
        appointment already in the calendar.
        """

        def _appointments_conflict(app_1, app_2):
            """Determine if a pair of appointments conflict."""
            #check if any overlapping participants in the appointments;
            #if not, then there's no conflict
            party_1, party_2 = app_1._participants, app_2._participants 
            if not (set(party_1) & set(party_2)):
                return False

            #grab hours and minutes of start and end time and convert to ints
            sh_1, sm_1 = app_1._start_time[:-2].split(":")
            sh_1, sm_1 = int(sh_1), int(sm_1)
            eh_1, em_1 = app_1._end_time[:-2].split(":")
            eh_1, em_1 = int(eh_1), int(em_1)
            
            sh_2, sm_2 = app_2._start_time[:-2].split(":")
            sh_2, sm_2 = int(sh_2), int(sm_2)
            eh_2, em_2 = app_2._end_time[:-2].split(":")
            eh_2, em_2 = int(eh_2), int(em_2)


            if app_1._start_time[-2:] == "pm":
                if app_1._start_time[0:2] != "12":
                    sh_1 += 12
                    sh_1 = sh_1 % 24
            if app_1._end_time[-2:] == "pm":
                if app_1._start_time[0:2] != "12":
                    eh_1 += 12
                    eh_1 = eh_1 % 24
            if app_2._start_time[-2:] == "pm":
                if app_1._start_time[0:2] != "12":
                    sh_2 += 12
                    sh_2 = sh_2 % 24
            if app_2._end_time[-2:] == "pm":
                if app_1._start_time[0:2] != "12":
                    eh_2 += 12
                    eh_2 = eh_2 % 24

            if app_1._start_time[-2:] == "am":
                sh_1 = sh_1 % 12
            if app_1._end_time[-2:] == "am":
                eh_1 = eh_1 % 12
            if app_2._start_time[-2:] == "am":
                sh_2 = sh_2 % 12
            if app_2._end_time[-2:] == "am":
                eh_2 = eh_2 % 12


            print app_1._start_time, app_1._end_time
            print app_2._start_time, app_2._end_time
            print sh_1, eh_1, sh_2, eh_2

        #grab all appointments in a list
        appointments = self._calendar.values()

        #if a single appointment conflicts with appointment X,
        #there's a conflict
        for appointment in appointments:
            if _appointments_conflict(appointment, X):
                return True

        return False
        
    def _handle_conflict(self):
        """Execute conflict resolution protocol."""
        pass

    def insert(self, X):
        """Insert Appointment X into this Node's local calendar and log."""
        #ensure we're inserting an Appointment before anything.
        if not isinstance(X, Appointment):
            raise TypeError("X must be of type Appointment.")

        #TODO: find out if clock only gets updated in event of successful insertion
        #increment clock and update this Node's time table
        self._clock += 1
        self._T[self._id][self._id] = self._clock

        #if the event doesn't conflict with anything currently in the
        #local calendar
        if not self._is_conflicting(X):
            
            #add appointment to calendar using appointment name as key as
            #we have assumed unique names for appointments.
            self._calendar[X._name] = X

            #create Event object for the insertion of this appointment
            #and place it in the log if it's not in the log already
            e = Event("INSERT", self._clock, self._id, X)
            if e not in self._log:
                self._log.append(e)
        else:
            #event conflicts with local calendar, 
            #execute conflict resolution protocol
            self._handle_conflict()