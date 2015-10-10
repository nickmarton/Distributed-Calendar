"""Event Record class for Distributed Systems Project 1."""

from Appointment import Appointment

class Event(object):
    """Event class."""

    def __init__(self, op, time, node_id, appointment):
        """Initialize a new Event object for a particular Node."""
        #Do type checking before anything.
        if not isinstance(op, str):
            raise TypeError("op parameter must be of type str")
        if not isinstance(time, int):
            raise TypeError("time parameter must be of type int")
        if not isinstance(node_id, int):
            raise TypeError("node_id parameter must be of type int")
        if not isinstance(appointment, Appointment):
            raise TypeError(
                "appointment parameter must be of type Appointment")

        valid_ops = ["INSERT", r"DELETE", "SEND", "RECEIVE"]

        #check if operation provided is valid
        if op.upper() not in valid_ops:
            raise ValueError(
                "operation must be either INSERT, DELETE, SEND, or RECEIVE")
        
        self._op = op
        self._time = time
        self._node_id = node_id
        self._appointment = appointment

    def __eq__(self, other):
        """Determine if two Event objects are equal."""
        c_op = self._op == other._op
        c_time = self._time == other._time
        c_id = self._node_id == other._node_id
        c_name = self._appointment._name == other._appointment._name

        return c_op and c_time and c_id and c_name

    def __ne__(self, other):
        """Determine if two Event objects are not equal."""
        return not __eq__(self, other)

    def __str__(self):
        """Creeate human-readable string representation of Event object."""
        repr_str = self._op + "(" + self._appointment._name + "), "
        repr_str += str(self._time) + ", " + str(self._node_id)
        return repr_str

    def __repr__(self):
        """Create machine representation of Event object."""
        return __str__(self)
