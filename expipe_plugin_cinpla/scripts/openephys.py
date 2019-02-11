from expipe_plugin_cinpla.imports import *
from expipe_plugin_cinpla.scripts.utils import _get_data_path
from expipe_io_neuro.openephys.openephys import generate_tracking, generate_events
from . import utils
from pathlib import Path
import shutil
import time
import os
import tempfile
import stat


def register_openephys_recording(
    project, action_id, openephys_path, depth, overwrite, templates,
    entity_id, user, session, location, message, tag, delete_raw_data,
    correct_depth_answer, register_depth):
    user = user or PAR.USERNAME
    if user is None:
        print('Missing option "user".')
        return
    location = location or PAR.LOCATION
    if location is None:
        print('Missing option "location".')
        return
    openephys_path = pathlib.Path(openephys_path)
    openephys_dirname = openephys_path.stem
    openephys_file = pyopenephys.File(str(openephys_path))
    openephys_exp = openephys_file.experiments[0]
    openephys_rec = openephys_exp.recordings[0]
    entity_id = entity_id or str(openephys_dirname).split('_')[0]
    session = session or str(openephys_dirname).split('_')[-1]
    if session.isdigit():
        pass
    else:
        print('Missing option "session".')
        return
    if action_id is None:
        session_dtime = datetime.datetime.strftime(openephys_exp.datetime, '%d%m%y')
        action_id = entity_id + '-' + session_dtime + '-' + session
    print('Generating action', action_id)
    try:
        action = project.create_action(action_id)
    except KeyError as e:
        if overwrite:
            project.delete_action(action_id)
            action = project.create_action(action_id)
        else:
            print(str(e) + ' Use "overwrite"')
            return
    action.datetime = openephys_exp.datetime
    action.type = 'Recording'
    action.tags.extend(list(tag) + ['open-ephys'])
    print('Registering entity id ' + entity_id)
    action.entities = [entity_id]
    print('Registering user ' + user)
    action.users = [user]
    print('Registering location ' + location)
    action.location = location

    if register_depth:
        correct_depth = utils.register_depth(
            project=project, action=action, depth=depth,
            answer=correct_depth_answer)
        if not correct_depth:
            print('Aborting registration!')
            project.delete_action(action_id)
            return
    utils.register_templates(action, templates)
    if message:
        action.create_message(text=message, user=user, datetime=datetime.datetime.now())

    for idx, m in enumerate(openephys_rec.messages):
        print('OpenEphys message: ', m.text)
        secs = float(m.time.rescale('s').magnitude)
        dtime = openephys_rec.datetime + datetime.timedelta(seconds=secs)
        action.create_message(text=m.text, user=user, datetime=dtime)

    exdir_path = utils._make_data_path(action, overwrite)
    openephys_io.convert(
        openephys_rec, exdir_path=exdir_path, session=session)
    if utils.query_yes_no(
        'Delete raw data in {}? (yes/no)'.format(openephys_path),
        default='no', answer=delete_raw_data):
        shutil.rmtree(openephys_path)


def process_openephys(project, action_id, probe_path, sorter, acquisition_folder=None,
                      exdir_file_path=None, spikesort=True, compute_lfp=True, compute_mua=False,
                      spikesorter_params=None, server=None, ground=None, ref=None, split=None,
                      ms_before_wf=1, ms_after_wf=2,):
    import spikeextractors as se
    import spiketoolkit as st

    proc_start = time.time()

    if server is None or server == 'local':
        if acquisition_folder is None:
            action = project.actions[action_id]
            # if exdir_path is None:
            exdir_path = _get_data_path(action)
            exdir_file = exdir.File(exdir_path, plugins=exdir.plugins.quantities)
            acquisition = exdir_file["acquisition"]
            if acquisition.attrs['acquisition_system'] is None:
                raise ValueError('No Open Ephys aquisition system ' +
                                 'related to this action')
            openephys_session = acquisition.attrs["session"]
            openephys_path = Path(acquisition.directory) / openephys_session
        else:
            openephys_path = Path(acquisition_folder)
            assert exdir_file_path is not None
            exdir_path = Path(exdir_file_path)

        probe_path = probe_path or project.config.get('probe')
        recording = se.OpenEphysRecordingExtractor(str(openephys_path))
        if ground is not None:
            active_channels = []
            for chan in recording.getChannelIds():
                if chan not in ground:
                    active_channels.append(chan)
            recording_active = se.SubRecordingExtractor(recording, channel_ids=active_channels)
        else:
            recording_active = recording

        print("Active channels: ", len(recording_active.getChannelIds()))

        # apply filtering and cmr
        print('Writing filtered and common referenced data')

        recording_hp = st.preprocessing.bandpass_filter(recording_active, freq_min=300, freq_max=6000)
        if ref is not None:
            if ref.lower() == 'cmr':
                reference = 'median'
            elif ref.lower() == 'car':
                reference = 'average'
            else:
                raise Exception("'reference' can be either 'cmr' or 'car'")
            if split == 'all':
                recording_cmr = st.preprocessing.common_reference(recording_hp, reference=reference)
            elif split == 'half':
                groups = [recording.getChannelIds()[:int(len(recording.getChannelIds()) / 2)],
                          recording.getChannelIds()[int(len(recording.getChannelIds()) / 2):]]
                recording_cmr = st.preprocessing.common_reference(recording_hp, groups=groups, reference=reference)
            else:
                if isinstance(split, list):
                    recording_cmr = st.preprocessing.common_reference(recording_hp, groups=split, reference=reference)
                else:
                    raise Exception("'split' must be a list of lists")
        else:
            recording_cmr = recording

        recording_lfp = st.preprocessing.bandpass_filter(recording_active, freq_min=1, freq_max=300)
        recording_lfp = st.preprocessing.resample(recording_lfp, 1000)
        recording_mua = st.preprocessing.resample(st.preprocessing.rectify(recording_cmr), 1000)
        tmpdir = Path(tempfile.mkdtemp(dir=os.getcwd()))

        if spikesort:
            print('Bandpass filter')
            t_start = time.time()

            filt_filename = Path(tmpdir) / 'filt.dat'
            se.BinDatRecordingExtractor.writeRecording(recording_cmr, save_path=filt_filename)
            recording_cmr = se.BinDatRecordingExtractor(filt_filename, samplerate=recording_cmr.getSamplingFrequency(),
                                                        numchan=len(recording_cmr.getChannelIds()))
            print('Filter time: ', time.time() -t_start)
        if compute_lfp:
            print('Computing LFP')
            t_start = time.time()
            lfp_filename = Path(tmpdir) / 'lfp.dat'
            se.BinDatRecordingExtractor.writeRecording(recording_lfp, save_path=lfp_filename)
            recording_lfp = se.BinDatRecordingExtractor(lfp_filename, samplerate=recording_lfp.getSamplingFrequency(),
                                                        numchan=len(recording_lfp.getChannelIds()))
            print('Filter time: ', time.time() -t_start)

        if compute_mua:
            print('Computing MUA')
            t_start = time.time()
            mua_filename =  Path(tmpdir) / 'mua.dat'
            se.BinDatRecordingExtractor.writeRecording(recording_mua, save_path=mua_filename)
            recording_mua = se.BinDatRecordingExtractor(mua_filename, samplerate=recording_mua.getSamplingFrequency(),
                                                        numchan=len(recording_mua.getChannelIds()))
            print('Filter time: ', time.time() -t_start)

        recording_cmr = se.loadProbeFile(recording_cmr, probe_path)
        recording_lfp = se.loadProbeFile(recording_lfp, probe_path)
        recording_mua = se.loadProbeFile(recording_mua, probe_path)

        if spikesort:
            try:
                if sorter == 'klusta':
                    sorting = st.sorters.klusta(recording_cmr, by_property='group', **spikesorter_params)
                elif sorter == 'mountain':
                    sorting = st.sorters.mountainsort4(recording_cmr, by_property='group', **spikesorter_params) # by_property='group',
                elif sorter == 'kilosort':
                    sorting = st.sorters.kilosort(recording_cmr, by_property='group', **spikesorter_params)
                elif sorter == 'spyking-circus':
                    sorting = st.sorters.spyking_circus(recording_cmr, by_property='group', **spikesorter_params)
                elif sorter == 'ironclust':
                    sorting = st.sorters.ironclust(recording_cmr, by_property='group', **spikesorter_params)
                else:
                    raise NotImplementedError("sorter is not implemented")
            except Exception as e:
                try:
                    shutil.rmtree(tmpdir)
                except TypeError as te:
                    pass
                print(e)
                raise Exception("Spike sorting failed")
            print('Found ', len(sorting.getUnitIds()), ' units!')

        # extract waveforms
        if spikesort:
            print('Computing waveforms')
            wf = st.postprocessing.getUnitWaveforms(recording_cmr, sorting, by_property='group',
                                                    ms_before=ms_before_wf, ms_after=ms_after_wf, verbose=True)
            print('Saving sorting output to exdir format')
            se.ExdirSortingExtractor.writeSorting(sorting, exdir_path, recording=recording_cmr)
        if compute_lfp:
            print('Saving LFP to exdir format')
            se.ExdirRecordingExtractor.writeRecording(recording_lfp, exdir_path, lfp=True)
        if compute_mua:
            print('Saving MUA to exdir format')
            se.ExdirRecordingExtractor.writeRecording(recording_mua, exdir_path, mua=True)

        # check for tracking
        oe_recording = pyopenephys.File(str(openephys_path)).experiments[0].recordings[0]
        if len(oe_recording.tracking) > 0:
            print('Saving ', len(oe_recording.tracking), ' Open Ephys tracking sources')
            generate_tracking(exdir_path, oe_recording)

        if len(oe_recording.events) > 0:
            print('Saving ', len(oe_recording.events), ' Open Ephys event sources')
            generate_events(exdir_path, oe_recording)

        print('Cleanup')
        if not os.access(str(tmpdir), os.W_OK):
            # Is the error an access error ?
            os.chmod(str(tmpdir), stat.S_IWUSR)
        try:
            shutil.rmtree(str(tmpdir), ignore_errors=True)
        except:
            print('Could not remove ', str(tmpdir))

    else:
        config = expipe.config._load_config_by_name(None)
        assert server in [s['host'] for s in config.get('servers')]
        server_dict = [s for s in config.get('servers') if s['host'] == server][0]
        host = server_dict['domain']
        user = server_dict['user']
        password = server_dict['password']
        port = 22

        # host, user, pas, port = utils.get_login(
        #     hostname=hostname, username=username, port=port, password=password)
        ssh, scp_client, sftp_client, pbar = utils.login(
            hostname=host, username=user, password=password, port=port)
        print('Invoking remote shell')
        remote_shell = utils.ShellHandler(ssh)

        ########################## SEND  #######################################
        action = project.actions[action_id]
        # if exdir_path is None:
        exdir_path = _get_data_path(action)
        exdir_file = exdir.File(exdir_path, plugins=exdir.plugins.quantities)
        acquisition = exdir_file["acquisition"]
        if acquisition.attrs['acquisition_system'] is None:
            raise ValueError('No Open Ephys aquisition system ' +
                             'related to this action')
        openephys_session = acquisition.attrs["session"]
        openephys_path = Path(acquisition.directory) / openephys_session
        print('Initializing transfer of "' + str(openephys_path) + '" to "' +
              host + '"')


        try:  # make directory for untaring
            process_folder = 'process_' + str(np.random.randint(10000000))
            stdin, stdout, stderr = remote_shell.execute('mkdir ' + process_folder)
        except IOError:
            pass
        print('Packing tar archive')
        remote_acq = process_folder + '/acquisition'
        remote_tar = process_folder + '/acquisition.tar'

        # transfer acquisition folder
        local_tar = shutil.make_archive(str(openephys_path), 'tar', str(openephys_path))
        print(local_tar)
        scp_client.put(
            local_tar, remote_tar, recursive=False)

        # transfer probe_file
        remote_probe = process_folder + '/probe.prb'
        scp_client.put(
            probe_path, remote_probe, recursive=False)

        remote_exdir = process_folder + '/main.exdir'
        remote_proc = process_folder + '/main.exdir/processing'
        remote_proc_tar = process_folder + '/processing.tar'
        local_proc = str(exdir_path / 'processing')
        local_proc_tar = local_proc + '.tar'

        # transfer spike params
        if spikesorter_params is not None:
            spike_params_file = 'spike_params.yaml'
            with open(spike_params_file, 'w') as f:
                yaml.dump(spikesorter_params, f)
            remote_yaml = process_folder + '/' + spike_params_file
            scp_client.put(
                spike_params_file, remote_yaml, recursive=False)
            try:
                os.remove(spike_params_file)
            except:
                print('Could not remove: ', spike_params_file)
        else:
            remote_yaml = 'none'

        extra_args = ""
        if not compute_lfp:
            extra_args = extra_args + ' --no-lfp'
        if not compute_mua:
            extra_args = extra_args + ' --no-mua'
        if not spikesort:
            extra_args = extra_args + ' --no-sorting'

        if ref is not None and isinstance(ref, str):
            ref = ref.lower()
        if split is not None and isinstance(split, str):
            split = split.lower()

        ground_cmd = ''
        if ground is not None:
            for g in ground:
                ground_cmd = ground_cmd + ' -g ' + str(g)

        ref_cmd = ''
        if ref is not None:
            ref_cmd = ' --ref ' + ref.lower()

        split_cmd = ''
        if split is not None:
            split_cmd = ' --split-channels ' + str(split)

        wf_cmd = ' --ms-before-wf ' + str(ms_before_wf) + ' --ms-after-wf ' + str(ms_after_wf)

        try:
            pbar[0].close()
        except Exception:
            pass

        print('Making acquisition folder')
        cmd = "mkdir " + remote_acq
        print('Shell: ', cmd)
        stdin, stdout, stderr = remote_shell.execute("mkdir " + remote_acq)
        # utils.ssh_execute(ssh, "mkdir " + remote_acq)

        print('Unpacking tar archive')
        cmd = "tar -xf " + remote_tar + " --directory " + remote_acq
        stdin, stdout, stderr = remote_shell.execute(cmd)
        # utils.ssh_execute(ssh, cmd)

        print('Deleting tar archives')
        sftp_client.remove(remote_tar)
        if not os.access(str(local_tar), os.W_OK):
            # Is the error an access error ?
            os.chmod(str(local_tar), stat.S_IWUSR)
        try:
            os.remove(local_tar)
        except:
            print('Could not remove: ', local_tar)


        ###################### PROCESS #######################################
        print('Processing on server')
        cmd = "expipe process openephys {} --probe-path {} --sorter {} --spike-params {}  " \
              "--acquisition {} --exdir-path {} {} {} {} {} {}".format(action_id, remote_probe, sorter, remote_yaml,
                                                                       remote_acq, remote_exdir, ground_cmd, ref_cmd,
                                                                       split_cmd, wf_cmd, extra_args)

        stdin, stdout, stderr = remote_shell.execute(cmd, print_lines=True)

        print('Finished remote processing')
        ####################### RETURN PROCESSED DATA #######################
        print('Initializing transfer of "' + remote_proc + '" to "' +
              local_proc + '"')
        print('Packing tar archive')
        cmd = "tar -C " + remote_exdir + " -cf " + remote_proc_tar + ' processing'
        stdin, stdout, stderr = remote_shell.execute(cmd, print_lines=True)
        # utils.ssh_execute(ssh, "tar -C " + remote_exdir + " -cf " + remote_proc_tar + ' processing')
        scp_client.get(remote_proc_tar, local_proc_tar,
                       recursive=False)
        try:
            pbar[0].close()
        except Exception:
            pass

        print('Unpacking tar archive')
        tar = tarfile.open(local_proc_tar)
        tar.extractall(str(exdir_path))
        # print('Deleting tar archives')
        if not os.access(str(local_proc_tar), os.W_OK):
            # Is the error an access error ?
            os.chmod(str(local_proc_tar), stat.S_IWUSR)
        try:
            os.remove(local_proc_tar)
        except:
            print('Could not remove: ', local_proc_tar)
        # sftp_client.remove(remote_proc_tar)
        print('Deleting remote process folder')
        cmd = "rm -r " + process_folder
        stdin, stdout, stderr = remote_shell.execute(cmd)

        #################### CLOSE UP #############################
        ssh.close()
        sftp_client.close()
        scp_client.close()

    print('Saved to exdir: ', exdir_path)
    print("Total elapsed time: ", time.time() - proc_start)
