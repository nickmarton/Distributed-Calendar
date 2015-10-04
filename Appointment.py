"""Appointment Event Class for Distributed Systems Project 1."""

def _validate_time(time):
    """
    Determine if time parameter provided is valid.

    time parameter must be of the form [digit][digit]:[digit][digit]{am,pm}
    where the hour must be <= 12 and the minutes must be in half hour
    increments.

    Raise ValueError otherwise.
    """

    #enforce start and end times as strings
    if not isinstance(time, str):
        raise TypeError("time parameters must be of type string.")

    #regex to determine if times provided are in correct format,
    #i.e., [digit][digit]:[digit][digit]
    import re
    pattern = r"\d\d:\d\d(am|pm)"
    pattern2 = r"\d:\d\d(am|pm)"
    if not re.match(pattern, time) and not re.match(pattern2, time):
        raise ValueError(
            "time parameters must be of form "
            "[digit]*[digit]:[digit][digit]{am,pm}.")

    hour, minutes = time[:-2].split(":")

    if int(hour) < 0 or int(hour) > 12:
        raise ValueError(
            "hour digits must be between 0 and 12.")

    if int(minutes) % 30 != 0:
        raise ValueError("minute digits must be in 1/2 hour increments.")

def _validate_times(start_time, end_time):
    """
    Ensure start and end times conform to assumptions.

    events cannot span multiple days; therefore end_time must be
    strictly greater than start_time.
    """

    #grab hours and minutes of start and end time and convert to ints
    start_hour, start_minutes = start_time[:-2].split(":")
    start_hour, start_minutes = int(start_hour), int(start_minutes)
    end_hour, end_minutes = end_time[:-2].split(":")
    end_hour, end_minutes = int(end_hour), int(end_minutes)

    #convert to military time for easy comparison
    if start_time[-2:] == "pm":
        if start_time[0:2] != "12":
            start_hour += 12
            start_hour = start_hour % 24

    if end_time[-2:] == "pm":
        if end_time[0:2] != "12":
            end_hour += 12
            end_hour = end_hour % 24

    if start_time[-2:] == "am":
        start_hour = start_hour % 12

    if end_time[-2:] == "am":
        end_hour = end_hour % 12

    #handle case where end_time is 12:[xx]pm which gets modded to 0
    if start_time[-2:] == "am" and end_time[-2:] == "pm":
        return

    if start_hour < end_hour:
        return

    if start_hour == end_hour:
        if start_minutes < end_minutes:
            return

    raise ValueError(
        "start_time parameter must come "
        "strictly before end_time parameter.")

class Appointment(object):
    """
    Appointment class.

    name:           name of the appointment enforced as a string.
    day:            day of the appointment enforced as a string
                    matching some day of the week.
    start_time:     start time of the appointment enforced as a string of the
                    form [digit]*[digit]:[digit][digit]{am,pm}.
    end_time:       end time of the appointment enforced as a string of the
                    form [digit]*[digit]:[digit][digit]{am,pm}
    participants:   list of participants in the appointment.
    """

    def __init__(self, name, day, start_time, end_time, participants):
        """Initialize an event object."""
        #enforce name and day as strings
        if not isinstance(name, str):
            raise TypeError("name parameter must be of type string.")
        if not isinstance(day, str):
            raise TypeError("day parameter must be of type string.")

        days = ["sunday", "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday"]

        #enforce day as a valid day of the week
        if day.lower() not in days:
            raise ValueError("day parameter must be a day of the week.")

        #ensure provided times are syntactically correct
        _validate_time(start_time)
        _validate_time(end_time)

        #ensure provided times are semantically correct
        _validate_times(start_time, end_time)

        if not isinstance(participants, list):
            raise TypeError(
                "participants parameter must be of type list")

        self._name = name
        self._day = day
        self._start_time = start_time
        self._end_time = end_time
        self._participants = participants

    def __str__(self):
        """Convert event object to human readable string representation."""
        appointment_str = "Appointment \"" + self._name + "\" on "
        appointment_str += self._day[0].upper() + self._day[1:] + " from "
        appointment_str += self._start_time + " to " + self._end_time + " with "
        if len(self._participants) > 2:
            appointment_str += "".join(
                [str(i) + ", " for i in self._participants[:-1]])
            appointment_str += "and " + str(self._participants[-1])
        elif len(self._participants) == 2:
            appointment_str += str(self._participants[0]) + " and "
            appointment_str += str(self._participants[1])
        else:
            appointment_str += str(self._participants[0])

        return appointment_str
