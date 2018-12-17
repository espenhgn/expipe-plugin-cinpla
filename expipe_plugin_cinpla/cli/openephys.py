from expipe_plugin_cinpla.imports import *
from expipe_plugin_cinpla.scripts import openephys
from . import utils


def attach_to_cli(cli):
    @cli.command('openephys',
                 short_help='Register an open-ephys recording-action to database.')
    @click.argument('openephys-path', type=click.Path(exists=True))
    @click.option('-u', '--user',
                  type=click.STRING,
                  help='The experimenter performing the recording.',
                  )
    @click.option('-d', '--depth',
                  multiple=True,
                  callback=utils.validate_depth,
                  help=(
                    'Alternative "find" to find from surgery or adjustment' +
                    ' or given as <key num depth unit> e.g. ' +
                    '<mecl 0 10 um> (omit <>).'),
                  )
    @click.option('-l', '--location',
                  type=click.STRING,
                  callback=utils.optional_choice,
                  envvar=PAR.POSSIBLE_LOCATIONS,
                  help='The location of the recording, i.e. "room-1-ibv".'
                  )
    @click.option('--session',
                  type=click.STRING,
                  help='Session number, assumed to be in end of filename by default',
                  )
    @click.option('--action-id',
                  type=click.STRING,
                  help=('Desired action id for this action, if none' +
                        ', it is generated from open-ephys-path.'),
                  )
    @click.option('--entity-id',
                  type=click.STRING,
                  help='The id number of the entity.',
                  )
    @click.option('-m', '--message',
                  type=click.STRING,
                  help='Add message, use "text here" for sentences.',
                  )
    @click.option('-t', '--tag',
                  multiple=True,
                  type=click.STRING,
                  callback=utils.optional_choice,
                  envvar=PAR.POSSIBLE_TAGS,
                  help='Add tags to action.',
                  )
    @click.option('--overwrite',
                  is_flag=True,
                  help='Overwrite files and expipe action.',
                  )
    @click.option('--register-depth',
                  is_flag=True,
                  help='Overwrite files and expipe action.',
                  )
    @click.option('--templates',
                  multiple=True,
                  type=click.STRING,
                  help='Which templates to add',
                  )
    def _register_openephys_recording(
        action_id, openephys_path, depth, overwrite, templates,
        entity_id, user, session, location, message, tag, register_depth):
        openephys.register_openephys_recording(
            project=PAR.PROJECT,
            action_id=action_id,
            openephys_path=openephys_path,
            depth=depth,
            overwrite=overwrite,
            templates=templates,
            entity_id=entity_id,
            user=user,
            session=session,
            location=location,
            message=message,
            tag=tag,
            delete_raw_data=None,
            correct_depth_answer=None,
            register_depth=register_depth)

    @cli.command('process',
                 short_help='Filter data, spike sort, create LFP and MUA file output.')
    @click.argument('action-id', type=click.STRING)
    @click.option('--probe-path',
                  type=click.STRING,
                  help='Path to probefile, assumed to be in expipe config directory by default.',
                  )
    @click.option('--sorter',
                  default='klusta',
                  type=click.Choice(['klusta', 'mountain', 'kilosort', 'spyking-circus', 'ironclust']),
                  help='',
                  )
    def _process_openephys(action_id, probe_path, sorter):
        openephys.process_openephys(PAR.PROJECT, action_id, probe_path, sorter)



    @cli.command('psychopy',
                 short_help='process psychopy data')
    @click.argument('action-id', type=click.STRING)
    def _process_psychopy(action_id):
        '''
        keys = ["image", "sparsenoise", "grating", "sparsenoise", "movie"]
        valuekeys = ["duration", "image", "phase", "spatial_frequency", "frequency", "orientation", "movie"]
        {"image": {"duration": 0.25, "image": "..\\datasets\\converted_images\\image0004.png"}}
        {"sparsenoise": {"duration": 0.25, "image": "..\\datasets\\sparse_noise_images\\image0022.png"}}
        {"grating": {"duration": 0.25, "phase": 0.5, "spatial_frequency": 0.16, "frequency": 0, "orientation": 120}}
        {"movie": {"movie": "..\\datasets\\converted_movies\\segment1.mp4"}}
        {"grating": {"phase": "f*t", "duration": 2.0, "spatial_frequency": 0.04, "frequency": 4, "orientation": 225}}
        {"grayscreen" : {"duration": 300.}}
        '''
        
        from expipe_plugin_cinpla.scripts.utils import _get_data_path
        from pathlib import Path
        import pandas
        import os

        
        project = PAR.PROJECT
        action = project.actions[action_id]
        exdir_path = _get_data_path(action)
        exdir_file = exdir.File(exdir_path, plugins=exdir.plugins.quantities)
        acquisition = exdir_file["acquisition"]
        if acquisition.attrs['acquisition_system'] is None:
            raise ValueError('No Open Ephys aquisition system ' +
                             'related to this action')
        openephys_session = acquisition.attrs["openephys_session"]
        openephys_path = Path(acquisition.directory) / openephys_session

        openephys_file = pyopenephys.File(str(openephys_path))
        openephys_exp = openephys_file.experiments[0]
        openephys_rec = openephys_exp.recordings[0]


        print('Converting PsychoPy visual stimulation output to ".exdir"')
        stimfiles = glob.glob(os.path.join(str(openephys_path), '*.jsonl'))
        if len(stimfiles) == 0:
            raise Exception('Found no .jsonl file in folder {}'.format(openephys_path))
        if len(stimfiles) > 1:
            raise Exception('Found more than one .jsonl file in folder {}'.format(stimfiles))
        jsonl = [] # container
        for stimf in stimfiles:
            with open(stimf, 'r') as f:
                for line in f.readlines():
                    line = line.replace("'", '"')
                    try:
                        jsonl.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass # skip lines with non-json output
        ephocs = exdir_file.require_group('epochs')
        visual = ephocs.require_group('visual_stimulus')
        keys = []
        for stim in jsonl:
            keys += list(stim.keys())
        keys = np.array(keys)
        ttl_times = openephys_rec.events[0].times
        ttl_times = ttl_times[openephys_rec.events[0].full_words==128]
        try:
            assert(keys.size == ttl_times.size)
        except AssertionError:
            warnings.warn('number of ttl events ({}) do not match number of visual stimuli ({}), '.format(ttl_times.size, keys.size))
            if keys.size < ttl_times.size:
                warnings.warn('discarding {} last events'.format(ttl_times-keys.size))
                ttl_times = ttl_times[:keys.size]
            elif ttl_times.size < keys.size:
                warnings.warn('discarding {} last visual stimuli events'.format(keys.size - ttl_times.size))
                keys = keys[:ttl_times.size]
                jsonl = jsonl[:ttl_times.size]
        for key in np.unique(keys):
            stim = visual.require_group(key)
            stim.attrs['start_time'] = 0 * pq.s
            stim.attrs['stop_time'] = openephys_rec.duration
            num_samples = (keys == key).sum()
            dset = stim.require_dataset('times', data=ttl_times[keys == key])
            dset.attrs['num_samples'] = num_samples
            df = pandas.DataFrame([v[key] for v in np.array(jsonl)[keys == key]])
            for k in df.keys():
                if df[k].values.dtype == 'object':
                    # recast as string array
                    dset = stim.require_dataset(k, data=df[k].values.astype(str))
                else:
                    dset = stim.require_dataset(k, data=df[k].values)
                dset.attrs['num_samples'] = num_samples




    @cli.command('mousexy',
                 short_help='process mousexy data')
    @click.argument('action-id', type=click.STRING)
    @click.option('--mousexy-time-offset',
                  type=click.FLOAT,
                  default=-10.,
                  help='time relative to first TTL event the manymouse start to gather trackball motion. Default is -10 seconds')
    def _process_mousexy(action_id, mousexy_time_offset):

        from expipe_plugin_cinpla.scripts.utils import _get_data_path
        from pathlib import Path
        import os

        def get_trackballdata(pth):
            trackfiles = glob.glob(os.path.join(str(pth), '*.mousexy'))
            if len(trackfiles) == 0:
                raise Exception('Found no .mousexy file in folder {}'.format(pth))
            if len(trackfiles) > 1:
                raise Exception('Found more than one .mousexy file in folder {}'.format(trackfiles))
            jsonl = [] # container
            for track in trackfiles:
                with open(track, 'r') as f:
                    for line in f.readlines():
                        line = line.replace("'", '"')
                        try:
                            jsonl.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass # skip lines with non-json output
                # convert to structured array
                dtype = [('id', 'U8'), ('motion', 'U8'), ('time', '<f4'),
                    ('direction', 'U8'), ('value', '<i4')]
                trackballdata = []
                for j in jsonl:
                    for key, val in j.items():
                        l = [key] + [val['motion'], val['t'], 'X' if 'X' in list(val.keys()) else 'Y', val['X' if 'X' in list(val.keys()) else 'Y']]
                        trackballdata.append(tuple(l))
                return np.array(trackballdata, dtype=dtype)
            
        def generate_tracking(openephys_path, openephys_rec, exdir_file, ttl_time=0.*pq.s):
            trackballdata = get_trackballdata(pth=openephys_path)
            trackballdata['time'] += ttl_time # correct time stamps
            tracking_ = exdir_file.require_group('tracking')
            trackball = tracking_.require_group('trackball')
            position = trackball.require_group("position")
            position.attrs['start_time'] = 0 * pq.s
            position.attrs['stop_time'] = openephys_rec.duration
            for id in np.unique(trackballdata['id']):
                iinds = trackballdata['id'] == id
                data = trackballdata[iinds]
                for axis in 'XY':
                    led = position.require_group(id.replace('#', 'USB') + '_{}'.format(axis))
                    led.attrs['start_time'] = 0 * pq.s
                    led.attrs['stop_time'] = openephys_rec.duration
                    inds = data['direction'] == axis
                    dset = led.require_dataset('data', data=data['value'][inds].cumsum()*pq.dimensionless)
                    dset.attrs['num_samples'] = inds.sum()
                    dset = led.require_dataset('times', data=data['time'][inds]*pq.s)   # TODO: GRAB P-PORT EVENT - 10 s as mouse recording is always 10 s before the first visual stimulus
                    dset.attrs['num_samples'] = inds.sum()


        
        project = PAR.PROJECT
        action = project.actions[action_id]
        exdir_path = _get_data_path(action)
        exdir_file = exdir.File(exdir_path, plugins=exdir.plugins.quantities)
        acquisition = exdir_file["acquisition"]
        if acquisition.attrs['acquisition_system'] is None:
            raise ValueError('No Open Ephys aquisition system ' +
                             'related to this action')
        openephys_session = acquisition.attrs["openephys_session"]
        openephys_path = Path(acquisition.directory) / openephys_session
        
        openephys_file = pyopenephys.File(str(openephys_path))
        openephys_exp = openephys_file.experiments[0]
        openephys_rec = openephys_exp.recordings[0]
            
        ttl_times = openephys_rec.events[0].times
        ttl_times = ttl_times[openephys_rec.events[0].full_words==128]
        if len(ttl_times) != 0:
            ttl_time = ttl_times[0] + mousexy_time_offset*pq.s
        else:
            warnings.warn('No TTL events found!')
            ttl_time = mousexy_time_offset*pq.s
        print('Converting tracking from trackball (manymouse) raw data to ".exdir"')
        generate_tracking(openephys_path, openephys_rec, exdir_file, ttl_time)
