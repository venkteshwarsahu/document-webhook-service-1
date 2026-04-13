class InvalidDateException(Exception):
    """
        Exception raised for errors in the input date.
        Attributes:
            date -- input date which caused the error
    """

    def __init__(self, date) -> None:
        self.date = date
        super().__init__(self.date)
        