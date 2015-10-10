"""Event Record class for Distributed Systems Project 1."""

from Appointment import Appointment

class Event(object):
    """Event class."""

    def __init__(self, op, op_params, time, node_id):
        """Initialize a new Event object for a particular Node."""
        
        def _valid_comm_params(params):
            """Determine if params is a valid 2-tuple of an event-log and 2DTT."""
            #if params is not a tuple, invalid params
            if not isinstance(params, tuple):
                return False
            #if params is not a 2-tuple, invalid params
            if len(params) != 2:
                return False

            log, time_table = params


            #log must be a list of events
            if not isinstance(log, list):
                return False
            #if log is a list but something in it is not an event
            for e in log:
                if not isinstance(e, Event):
                    return False

            #if time_table is not a list, invalid params
            if not isinstance(time_table, list):
                return False

            #if some row is not a list, invalid params
            for row in time_table:
                if not isinstance(row, list):
                    return False

            #if there's an entry in the time table that's not an integer >= 0, invalid
            for row in time_table:
                for entry in row:
                    if not isinstance(entry, int):
                        return False
                    if entry < 0:
                        return False

            return True

        #Do type checking before anything.
        if not isinstance(op, str):
            raise TypeError("op parameter must be of type str")
        if not isinstance(time, int):
            raise TypeError("time parameter must be of type int")
        if not isinstance(node_id, int):
            raise TypeError("node_id parameter must be of type int")

        valid_ops = ["INSERT", r"DELETE", "SEND", "RECEIVE"]

        #check if operation provided is valid
        if op.upper() not in valid_ops:
            raise ValueError(
                "operation must be either INSERT, DELETE, SEND, or RECEIVE")
        
        import copy

        #if op is INSERT or DELETE
        if op in valid_ops[0:2]:
            #if op_params is not an appointment, raise TypeError
            if not isinstance(op_params, Appointment):
                raise TypeError(
                    "op_params must be of type Appointment for "
                    r"INSERT and DELETE operations")

            #make a copy of op_params Appointment object to be safe
            params = copy.deepcopy(op_params)

        #if op is SEND or RECEIVE
        if op in valid_ops[2:4]:
            #if op_params is not a 2-tuple (event-log, 2DTT) raise Error
            if not _valid_comm_params(op_params):
                raise ValueError(
                    "op_params must be a 2tuple of an event-log and 2DTT for "
                    "SEND and RECEIVE operations")

            #make a copy of op_params Appointment object to be safe
            params = copy.deepcopy(op_params)

        self._op = op
        self._op_params = params
        self._time = time
        self._node_id = node_id

    def __eq__(self, other):
        """Determine if two Event objects are equal."""
        
        def _eq_params(params1, params2):
            """Determine if parameters of two matching ops are equal."""

            #if op is INSERT or DELETE, compare Appointment objects
            if isinstance(params1, Appointment) and isinstance(params2, Appointment):
                return params1 == params2

            #extract respective logs and 2DTTs
            log1, time_table1 = params1
            log2, time_table2 = params2

            #TODO: determine if logs are equal when ordered or unordered
            #if logs aren't exactly the same
            if len(log1) != len(log2):
                return False

            for i in range(len(log1)):
                if log1[i] != log2[i]:
                    return False

            #time tables are assumed to have the same dimensions, check if
            #component-wise equivalent
            for i in range(len(time_table1)):
                for j in range(len(i)):
                    if time_table1[i][j] != time_table2[i][j]:
                        return False

            return True

        c_op = self._op == other._op

        if not c_op:
            return False

        c_time = self._time == other._time
        c_id = self._node_id == other._node_id
        #can't use equality operator here, offload param checking to function
        c_param = _eq_params(self._op_params, other._op_params)

        return c_time and c_id and c_param

    def __ne__(self, other):
        """Determine if two Event objects are not equal."""
        return not __eq__(self, other)

    def __str__(self):
        """Create human-readable string representation of Event object."""
        #decide on parenthetical expression for after op
        if self._op == "INSERT" or self._op == r"DELETE":
            ins_str = str(self._op_params._name)
        else:
            ins_str = "NP, T_" + str(self._node_id)
        
        repr_str = self._op + "(" + ins_str + "), "
        repr_str += str(self._time) + ", " + str(self._node_id)
        return repr_str

    def __repr__(self):
        """Create machine representation of Event object."""
        return self.__str__()
