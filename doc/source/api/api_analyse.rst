=======
Analyse
=======

The ModelScenario class allows users to collate related data sources and calculate
modelled output based on this data. The types of data currently included are:
 - Timeseries observation data (:ref:`ObsData<ObsData>`)
 - Fixed domain sensitivity maps known as footprints (:ref:`FootprintData<FootprintData>`)
 - Fixed domain flux maps (:ref:`FluxData<FluxData>`) - multiple maps can be included and referenced by source name
 - Fixed domain vertical curtains at the boundaries referred to as boundary conditions (:ref:`BoundaryConditionsData<BoundaryConditionsData>`)

.. autoclass:: openghg.analyse.ModelScenario
    :members:
