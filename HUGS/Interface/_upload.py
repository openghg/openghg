from HUGS.Client import Process, Search, Retrieve
from HUGS.Interface import Credentials

import ipywidgets as widgets
from pathlib import Path
import tempfile


class Upload:
    def __init__(self):
        self._credentials = Credentials()
        self._user = None

    def login(self):
        return self._credentials.login()

    def get_user(self):
        return self._credentials.get_user()

    def get_results(self):
        return self._results

    def upload(self):
        type_widget = widgets.Dropdown(
            options=["CRDS", "GC", "ICOS", "NOAA", "TB", "EUROCOM"],
            description="Data type:",
            disabled=False,
        )

        base_url = "https://hugs.acquire-aaai.com/t"


        upload_widget = widgets.FileUpload(multiple=False, label="Select")
        transfer_button = widgets.Button(
            description="Transfer", button_style="info", layout=widgets.Layout(width="10%")
        )

        def do_upload(a):
            if type_widget.value == False:
                status_text.value = f"<font color='red'>Please select a data type</font>"
                return

            user = self.get_user()
            data_type = type_widget.value

            if not user.is_logged_in():
                status_text.value = f"<font color='red'>User not logged in</font>"
                return

            # Here we get the data as bytes, write it to a tmp directory so we can
            # process it using HUGS
            # TODO - better processing method? Allow HUGS to accept bytes?
            with tempfile.TemporaryDirectory() as tmpdir:
                file_content = upload_widget.value
                filename = list(file_content.keys())[0]
                
                data_bytes = file_content[filename]["content"]

                tmp_filepath = Path(tmpdir).joinpath(filename) 

                with open(tmp_filepath, "wb") as f:
                   f.write(data_bytes)

                p = Process(service_url=base_url)
                result = p.process_files(user=user, files=tmp_filepath, data_type=data_type)

                self._results = result

                # Upload the file to HUGS
                if result:
                    status_text.value = f"<font color='green'>Upload successful</font>"
                else:
                    status_text.value = f"<font color='red'>Unable to process file</font>"

        transfer_button.on_click(do_upload)

        data_hbox = widgets.HBox(children=[type_widget, upload_widget, transfer_button])
        status_text = widgets.HTML(value=f"<font color='#00BCD4'>Waiting for file</font>")


        return widgets.VBox(children=[data_hbox, status_text])
