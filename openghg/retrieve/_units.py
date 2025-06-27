import pint
import pint_xarray

from openghg.dataobjects import (
    BoundaryConditionsData,
    FluxData,
    FootprintData,
    ObsData,
)

from openghg.util._file import load_internal_json


class AssignUnits:
    """
    Class for assigning pint units to OpenGHG objects
    """

    def __init__(
        self,
        data: ObsData | FootprintData | FluxData | BoundaryConditionsData,
    ):
        self.data = data

    def _openghg_unit_registry(self) -> pint.UnitRegistry:
        """
        Adapted pint unit registry for trace gas data
        and modelling in OpenGHG
        """
        ureg = pint.UnitRegistry(force_ndarray_like=True)
        ureg.define("ppb = 1e-9 mol/mol = parts_per_billion")
        ureg.define("ppt = 1e-12 mol/mol= parts_per_trillion")
        ureg.define("permeg = 0.001 permille")
        ureg.define("m2 = m*m = metres_squared")

        return ureg

    def read_attributes_json(self) -> dict:
        """
        Read in OpenGHG attributes.json file
        """
        attrs = load_internal_json("attributes.json")
        return attrs

    def _obs_units(self) -> ObsData | FootprintData | FluxData | BoundaryConditionsData:
        """
        Method for quantifying pint units to
        atmospheric trace gas observations
        """
        # Retrieve unit registry
        ureg = self._openghg_unit_registry()
        pint_xarray.accessors.default_registry = ureg

        # Read OpenGHG attributes file that maps units
        openghg_attrs = self.read_attributes_json()
        openghg_pint_unit_mag_mapping = openghg_attrs["unit_pint"]
        pint_unit_name_parser = openghg_attrs["unit_non_standard_interpret"]

        for key in self.data.data.keys():
            # For each observation parameter that has a unit attribute
            # we convert those unit attributes to pint format but
            # retain the xarray data structure

            if "units" in list(self.data.data[key].attrs.keys()):
                i_unit = self.data.data[key].attrs["units"]

                # Assign pint [mol/mol] format to the obs variables
                # NB. This will appear as dimensionless in the xarray
                # data variables, which is true but could be confusing
                if i_unit in list(openghg_pint_unit_mag_mapping.keys()):
                    pint_unit = openghg_pint_unit_mag_mapping[i_unit]
                    # Assign the pseudo-unit
                    self.data.data[key] = self.data.data[key].pint.quantify(ureg.parse_units(pint_unit))
                    # Convert data to mol/mol
                    self.data.data[key] = self.data.data[key].pint.to("mol/mol")

                elif i_unit == "mol/mol":
                    self.data.data[key] = self.data.data[key].pint.quantify(ureg.parse_units(i_unit))

                # Convert tracer, isotope or APO/O2 units to pint format
                elif i_unit in list(pint_unit_name_parser.keys()):
                    pint_unit = pint_unit_name_parser[i_unit]
                    self.data.data[key] = self.data.data[key].pint.quantify(ureg.parse_units(pint_unit))

                else:
                    raise ValueError(f"The unit {i_unit} is not recognised.")

        return self.data

    def _footprint_units(self) -> ObsData | FootprintData | FluxData | BoundaryConditionsData:
        """
        Method for quantifying pint units to
        atmospheric transport model data
        """
        # Retrieve unit registry
        ureg = self._openghg_unit_registry()
        pint_xarray.accessors.default_registry = ureg

        for key in list(self.data.data.keys()):
            if "units" in list(self.data.data[key].attrs.keys()):
                # Variable `air_temperature`` has 'C' as its unit.
                # This is designated to Coulombs in Pint.
                # Specify it actually is degrees C here
                if key == "air_temperature":
                    i_unit = "degC"

                # Pint doesn't recognise 'm s-1' as a unit, so specify here
                # TO DO - see if this can be supported through cf_xarray
                elif key == "wind_speed":
                    i_unit = "m/s"
                else:
                    i_unit = self.data.data[key].attrs["units"]

                try:
                    for coord in self.data.data[key].coords:
                        self.data.data[key][coord].attrs.pop("units", None)
                    self.data.data[key] = self.data.data[key].pint.quantify(ureg.parse_units(i_unit))
                except pint.errors.UndefinedUnitError:
                    print(f"Skipping {key} as pint could not parse {i_unit}")
                    pass

        return self.data

    def _isoflux_units(self) -> ObsData | FootprintData | FluxData | BoundaryConditionsData:
        """
        Method for quantifying pint units to
        (iso)flux data
        """
        # Retrieve unit registry
        ureg = self._openghg_unit_registry()
        pint_xarray.accessors.default_registry = ureg

        # Read OpenGHG attributes file that maps units
        openghg_attrs = self.read_attributes_json()
        pint_unit_name_parser = openghg_attrs["unit_non_standard_interpret"]

        # Assign flux pint unit
        flux_unit = self.data.data.flux.attrs["units"]

        # If isotope or oxygen data being used
        if flux_unit in pint_unit_name_parser:
            flux_unit = pint_unit_name_parser[flux_unit]

        self.data.data["flux"] = self.data.data["flux"].pint.quantify(ureg.parse_units(flux_unit))

        return self.data

    def _bc_units(self) -> ObsData | FootprintData | FluxData | BoundaryConditionsData:
        """
        Method for quantifying pint units to
        boundary conditions data
        """
        # Retrieve unit registry
        ureg = self._openghg_unit_registry()
        pint_xarray.accessors.default_registry = ureg

        # Assign BC pint unit (not for tracers)
        for c in ["n", "e", "s", "w"]:
            try:
                bc_unit = self.data.data[f"vmr_{c}"].attrs["units"]
            except KeyError:
                print("No units provided. Assuming units of mol/mol")
                bc_unit = "mol/mol"
            self.data.data[f"vmr_{c}"] = self.data.data[f"vmr_{c}"].pint.quantify(ureg.parse_units(bc_unit))

        return self.data

    def attribute(self) -> ObsData | FootprintData | FluxData | BoundaryConditionsData:
        """
        Method for attributing pint units to different
        OpenGHG dataobjects

        Returns:
            OpenGHG dataoject with assigned pint units
        """
        if type(self.data) is ObsData:
            return self._obs_units()

        elif type(self.data) is FootprintData:
            return self._footprint_units()

        elif type(self.data) is FluxData:
            return self._isoflux_units()

        elif type(self.data) is BoundaryConditionsData:
            return self._bc_units()
