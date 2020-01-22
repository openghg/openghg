x_scale = bq.DateScale()
y_scale = bq.LinearScale()

x_scale_inset = bq.DateScale()
y_scale_inset = bq.LinearScale()

ax = bq.Axis(label="Date", label_offset="50px", scale=x_scale, grid_lines="none")
ay = bq.Axis(label="Count", label_offset="50px", scale=y_scale, orientation="vertical", grid_lines="none")

ax_inset = bq.Axis(label="Date", label_offset="50px", scale=x_scale_inset, grid_lines="none")
ay_inset = bq.Axis(label="Count", label_offset="50px", scale=y_scale_inset, orientation="vertical", grid_lines="none")

# figure = bq.Figure(marks=[], axes=[ax,ay], animation_duration=1000)

scales = {"x": x_scale, "y": y_scale}

bsd = bq.Lines(scales=scales, colors=["#4E79A7"], stroke_width=1)
bsd.x = ch4_data["bsd_ch4_108m"]["time"]
bsd.y = ch4_data["bsd_ch4_108m"]["ch4_count"]

tac = bq.Lines(scales=scales, colors=["#f28e2b"], stroke_width=1)
tac.x = ch4_data["tac_ch4_100m"]["time"]
tac.y = ch4_data["tac_ch4_100m"]["ch4_count"]

inset_scale = {"x": x_scale_inset, "y": y_scale_inset}

bsd_inset = bq.Lines(scales=inset_scale, colors=["#4E79A7"], stroke_width=1)
bsd_inset.x = ch4_data["bsd_ch4_108m"]["time"]
bsd_inset.y = ch4_data["bsd_ch4_108m"]["ch4_count"]

tac_inset = bq.Lines(scales=inset_scale, colors=["#f28e2b"], stroke_width=1)
tac_inset.x = ch4_data["tac_ch4_100m"]["time"]
tac_inset.y = ch4_data["tac_ch4_100m"]["ch4_count"]

# Create a fast interval selector
intsel_fast = FastIntervalSelector(scale=x_scale, marks=[bsd, tac])


def fast_interval_change_callback(change):
    db_fast.value = 'The selected period is ' + str(change.new)

    start = pd.Timestamp(change.new[0])
    end = pd.Timestamp(change.new[1])

    x_scale_inset.min = start
    x_scale_inset.max = end


intsel_fast.observe(fast_interval_change_callback, names=['selected'])

# db_fast = ipw.HTML()
# db_fast.value = 'The selected period is ' + str(selector.selected)

m_fig = dict(left=100, top=50, bottom=50, right=100)

# This is where we assign the interaction to this particular Figure
figure = bq.Figure(marks=[bsd, tac], axes=[ax, ay], title='CH4 data',
                   interaction=intsel_fast, animation_duration=100, fig_margin=m_fig)

figure_inset = bq.Figure(marks=[bsd_inset, tac_inset], axes=[ax_inset, ay_inset],
                         title='Selected', animation_duration=10, fig_margin=m_fig)

figure.layout.width = "auto"
figure.layout.height = "auto"
figure.layout.min_height = "500px"


figure_inset.layout.width = "auto"
figure_inset.layout.height = "auto"
figure.layout.min_height = "400px"
figure.layout.min_width = "400px"


ipw.HBox(children=[figure, figure_inset])
