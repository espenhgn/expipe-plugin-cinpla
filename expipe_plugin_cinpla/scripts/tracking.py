from expipe_plugin_cinpla.imports import *
from expipe_plugin_cinpla.scripts.utils import _get_data_path
from expipe_io_neuro.openephys.openephys import generate_tracking, generate_events


def process_tracking(project, action_id, openephys_path):
        action = project.actions[action_id]
        # if exdir_path is None:
        exdir_path = _get_data_path(action)
        exdir_file = exdir.File(exdir_path, plugins=exdir.plugins.quantities)
        acquisition = exdir_file["acquisition"]
        if acquisition.attrs['acquisition_system'] is None:
            raise ValueError('No Open Ephys acquisition system ' +
                             'related to this action')
        session = acquisition.attrs["session"]

        # check for tracking
        oe_recording = pyopenephys.File(str(openephys_path)).experiments[0].recordings[0]
        if len(oe_recording.tracking) > 0:
            print('Saving ', len(oe_recording.tracking), ' Open Ephys tracking sources')
            generate_tracking(exdir_path, oe_recording)

        if len(oe_recording.events) > 0:
            print('Saving ', len(oe_recording.events), ' Open Ephys event sources')
            generate_events(exdir_path, oe_recording)

        openephys_io.convert_tracking(
            oe_recording, exdir_path=exdir_path, session=session)
