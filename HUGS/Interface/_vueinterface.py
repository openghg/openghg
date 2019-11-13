""" A class that's used to crete the ipyvuetify interface for
    use with Voila

    Not sure if this is the tidiest way to do this?
    
"""
from HUGS.Interface import Interface

class VueInterface:
    def __init__(self):
        self._interface = Interface()

