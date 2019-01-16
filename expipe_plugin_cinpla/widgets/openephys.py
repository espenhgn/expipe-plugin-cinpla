from expipe_plugin_cinpla.imports import *
from expipe_plugin_cinpla.scripts import openephys
from .utils import SelectDirectoryButton, MultiInput, SearchSelectMultiple, SelectFileButton, \
    required_values_filled, none_if_empty, split_tags, SearchSelect, ParameterSelectList


def openephys_view(project):
    openephys_path = SelectDirectoryButton(description='*Select OpenEphys path')
    user = ipywidgets.Text(placeholder='*User', value=PAR.USERNAME)
    session = ipywidgets.Text(placeholder='Session')
    location = ipywidgets.Text(placeholder='*Location', value=PAR.LOCATION)
    action_id = ipywidgets.Text(placeholder='Action id')
    entity_id = ipywidgets.Text(placeholder='Entity id')
    message = ipywidgets.Text(placeholder='Message')
    tag = ipywidgets.Text(placeholder='Tags (; to separate)')
    templates = SearchSelectMultiple(project.templates, description='Templates')
    depth = MultiInput(['Key', 'Probe', 'Depth', 'Unit'], 'Add depth')
    register_depth = ipywidgets.Checkbox(description='Register depth', value=False)
    register_depth_from_adjustment = ipywidgets.Checkbox(
        description='Find adjustments', value=True)

    overwrite = ipywidgets.Checkbox(description='Overwrite', value=False)
    delete_raw_data = ipywidgets.Checkbox(
        description='Delete raw data', value=False)
    register = ipywidgets.Button(description='Register')

    fields = ipywidgets.VBox([
        user,
        location,
        session,
        action_id,
        entity_id,
        message,
        tag,
        register
    ])
    checks = ipywidgets.HBox([openephys_path, register_depth, overwrite, delete_raw_data])
    main_box = ipywidgets.VBox([
            checks,
            ipywidgets.HBox([fields, templates])
        ])


    def on_register_depth(change):
         if change['name'] == 'value':
             if change['owner'].value:
                 children = list(checks.children)
                 children = children[:2] + [register_depth_from_adjustment] + children[2:]
                 checks.children = children
             else:
                children = list(checks.children)
                del(children[2])
                checks.children = children


    def on_register_depth_from_adjustment(change):
         if change['name'] == 'value':
             if not change['owner'].value:
                 children = list(fields.children)
                 children = children[:5] + [depth] + children[5:]
                 fields.children = children
             else:
                 children = list(fields.children)
                 del(children[5])
                 fields.children = children

    register_depth.observe(on_register_depth)
    register_depth_from_adjustment.observe(on_register_depth_from_adjustment)


    def on_register(change):
        if not required_values_filled(user, location, openephys_path):
            return
        tags = split_tags(tag)
        openephys.register_openephys_recording(
            templates=templates.value,
            project=project,
            action_id=none_if_empty(action_id.value),
            openephys_path=openephys_path.directory,
            depth=depth.value,
            overwrite=overwrite.value,
            register_depth=register_depth.value,
            entity_id=none_if_empty(entity_id.value),
            user=user.value,
            session=session.value,
            location=location.value,
            message=none_if_empty(message.value),
            tag=tags,
            delete_raw_data=delete_raw_data.value,
            correct_depth_answer=True)

    register.on_click(on_register)
    return main_box

#TODO add spike sorter specific params
def process_view(project):
    import spiketoolkit as st

    probe_path = SelectFileButton('.prb', description='*Select probe file', style={'description_width': 'initial'})
    action_id = SearchSelect(project.actions, description='*Actions')
    sorter = ipywidgets.Dropdown(
        description='Sorter', options=['klusta', 'mountain', 'kilosort', 'spyking-circus', 'ironclust'],
        style={'description_width': 'initial'}
    )
    params = st.sorters.klusta_default_params()
    sorter_param = ParameterSelectList(description='Spike sorting options', param_dict=params, layout={'width': '100%'})
    compute_lfp = ipywidgets.Checkbox(
        description='Compute LFP', value=True, style={'description_width': 'initial'})
    compute_mua = ipywidgets.Checkbox(
        description='Compute MUA', value=False, style={'description_width': 'initial'})
    spikesort = ipywidgets.Checkbox(
        description='Spike sort', value=True, style={'description_width': 'initial'})

    check_boxes = ipywidgets.VBox([ipywidgets.Label('Processing options', style={'description_width': 'initial'}),
                                   spikesort, compute_lfp, compute_mua],
                                  layout={'width': '30%'})

    run = ipywidgets.Button(description='Process', layout={'width': '100%', 'height': '100px'})
    run.style.font_weight = '100px'
    run.style.button_color = 'pink'

    fields = ipywidgets.VBox([
        probe_path,
        sorter,
        sorter_param
    ])

    main_box = ipywidgets.VBox([
            ipywidgets.HBox([fields, action_id, check_boxes]), run
        ], layout={'width': '100%'})
    main_box.layout.display = 'flex'

    def on_change(change):
        if change['type'] == 'change' and change['name'] == 'value':
            if sorter.value == 'klusta':
                params = st.sorters.klusta_default_params()
            elif sorter.value == 'mountain':
                params = st.sorters.mountainsort4_default_params()
            elif sorter.value == 'kilosort':
                params = st.sorters.kilosort_default_params()
            elif sorter.value == 'spyking-circus':
                params = st.sorters.spyking_circus_default_params()
            elif sorter.value == 'ironclust':
                params = st.sorters.ironclust_default_params()
            else:
                raise NotImplementedError("sorter is not implemented")
            sorter_param.update_params(params)

    def on_run(change):
        if not required_values_filled(probe_path, action_id):
            return
        spikesorter_params = sorter_param.value
        for (k, v) in spikesorter_params.items():
            if v == 'None':
                spikesorter_params[k] = None
        openephys.process_openephys(
            project=project,
            action_id=action_id.value,
            probe_path=probe_path.file,
            sorter=sorter.value,
            spikesort=spikesort.value,
            compute_lfp=compute_lfp.value,
            compute_mua=compute_mua.value,
            spikesorter_params=spikesorter_params)

    sorter.observe(on_change)
    run.on_click(on_run)
    return main_box
