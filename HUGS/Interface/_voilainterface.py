""" A class that's used to crete the ipyvuetify interface for
    use with Voila

"""
import bqplot as bq
import ipyvuetify as v
import ipywidgets as widgets

from HUGS.Interface import Interface

__all__ = ["VoilaInterface"]

class VoilaInterface:
    def __init__(self):
        # Create an Interface object, this handles the creation of ipywidgets and
        # widgets layout etc
        self._interface = Interface()
        
        self._interface.voila_interface()

    def voila_layout(self):
        """ Create the voila layout

        """
        register_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["account_circle"]), "  Register"])])
        login_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["check_circle_outline"]), "  Login"])])
        search_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["search"]), "  Search"])])
        plot_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["fa-bar-chart"]), "  Plotting"])])

        interface = self._interface

        # output_box = widgets.Output()

        def on_menu_click(widget, event, data):
            with output_box:
                print(f"Printing {widget}, event {event}")

        def on_register_click(widget, event, data):
            reg_layout.children = interface.show_module("register")

        def on_login_click(widget, event, data):
            reg_layout.children = interface.show_module("login")

        def on_search_click(widget, event, data):
            reg_layout.children = interface.show_module("search")

        def on_plot_click(widget, event, data):
            print(interface.show_module("plot_1"))
        #     reg_layout.children = interface.show_module("plot_1")

        register_nav.on_event("click", on_register_click)
        login_nav.on_event("click", on_login_click)
        search_nav.on_event("click", on_search_click)
        plot_nav.on_event("click", on_plot_click)

        nav_items = [register_nav, login_nav, search_nav, plot_nav]

        nav_drawer = v.NavigationDrawer(children=nav_items)

        nav_layout = v.Layout(_metadata={"mount_id": "content-nav"}, children=[nav_drawer])
        reg_layout = v.Layout(_metadata={"mount_id": "content-main"}, children=[])

        return v.Layout(children=[nav_layout, reg_layout])




