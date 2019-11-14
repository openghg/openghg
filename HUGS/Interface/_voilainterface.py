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
        # Create the voila interface
        self._interface.voila_interface()

    def interface_module(self, module_name):
        """ Cleaner interface to the Interface show_module function

            Args:
                module_name (str): Name of Interface module to return
            Returns:
                list: List containing ipywidgets VBox. The VBox is placed into a list
                as ipyvuetify requires the children attribute to be a list of widgets
        """
        return self._interface.show_module(module_name=module_name)

    def create_toolbar(self):
        """ Creates the toolbar for along the top of the page

            TODO - login / signup buttons could be added to this?

        """
        toolbar = v.Toolbar(app=True, dark=True, class_="teal", 
                            children=[v.ToolbarTitle(class_="headline", children=["HUGS"])])

        return toolbar

    # def create_nav_drawer(self):
    #     """ Create the ipyvuetify navigation drawer

    #         Returns:
    #             ipyvuetify.Layout: Navigation drawer components as part of Layout
    #     """
    #     register_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["account_circle"]), "  Register"])])
    #     login_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["check_circle_outline"]), "  Login"])])
    #     search_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["search"]), "  Search"])])
    #     plot_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["fa-bar-chart"]), "  Plotting"])])

    #     def on_register_click(widget, event, data):
    #         reg_layout.children = self.interface_module(module_name="register")

    #     def on_login_click(widget, event, data):
    #         reg_layout.children = self.interface_module(module_name="login")

    #     def on_search_click(widget, event, data):
    #         reg_layout.children = self.interface_module(module_name="search")

    #     def on_plot_click(widget, event, data):
    #         reg_layout.children = self.interface_module(moDocstring:      Widget that can be inserted into the DOMdule_name="plot_1")

    #     register_nav.on_event("click", on_register_click)
    #     login_nav.on_event("click", on_login_click)
    #     search_nav.on_event("click", on_search_click)
    #     plot_nav.on_event("click", on_plot_click)

    #     nav_items = [register_nav, login_nav, search_nav, plot_nav]
    #     nav_drawer = v.NavigationDrawer(children=nav_items)
    #     nav_layout = v.Layout(_metadata={"mount_id": "content-nav"}, children=[nav_drawer])

    #     return nav_layout

    def voila_layout(self):
        """ Create the voila layout

        """
        register_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["account_circle"]), "  Register"])])
        login_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["check_circle_outline"]), "  Login"])])
        search_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["search"]), "  Search"])])
        plot_nav = v.ListItem(children=[v.ListItemTitle(children=[v.Icon(children=["fa-bar-chart"]), "  Plotting"])])

        def on_menu_click(widget, event, data):
            with output_box:
                print(f"Printing {widget}, event {event}")

        def on_register_click(widget, event, data):
            reg_layout.children = self.interface_module(module_name="register")

        def on_login_click(widget, event, data):
            reg_layout.children = self.interface_module(module_name="login")

        def on_search_click(widget, event, data):
            # reg_layout.children = self.interface_module(module_name="search")
            reg_layout.children = self.search_select_layout()

        def on_plot_click(widget, event, data):
            print(self.interface_module(module_name="plot_1"))
        #     reg_layout.children = self.interface_module(module_name="plot_1")

        register_nav.on_event("click", on_register_click)
        login_nav.on_event("click", on_login_click)
        search_nav.on_event("click", on_search_click)
        plot_nav.on_event("click", on_plot_click)

        nav_items = [register_nav, login_nav, search_nav, plot_nav]

        nav_drawer = v.NavigationDrawer(children=nav_items)

        nav_layout = v.Layout(_metadata={"mount_id": "content-nav"}, children=[nav_drawer])
        reg_layout = v.Layout(_metadata={"mount_id": "content-main"}, children=[])

        return v.Layout(children=[nav_layout, reg_layout])

    
    def search_select_layout(self):
        """ Creates the ipyvuetify layout for the searching and data selection
            widgets

        """
        search_layout = v.Layout(children=self.interface_module(module_name="search"))
        data_layout = v.Layout(children=self.interface_module(module_name="download"))
        map_button = v.Btn(children=["Open map"])
        map_button_layout = v.Layout(chldren=map_button)
        map_layout = v.Layout(children=[])

        def show_map(*args):
            map_layout.children = self.interface_module(module_name="map")

        map_button.on_event("click", show_map)

        selection_layout = v.Layout(row=True, children=[search_layout, v.Spacer(), data_layout, map_button_layout, map_layout])
        
        return [selection_layout]





