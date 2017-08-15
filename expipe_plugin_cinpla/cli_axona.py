from .imports import *
from . import action_tools
from . import pytools


def attach_to_cli(cli):
    @cli.command('register', short_help='Generate an axona recording-action to database.')
    @click.argument('axona-filename', type=click.Path(exists=True))
    @click.option('-u', '--user',
                  type=click.STRING,
                  help='The experimenter performing the recording.',
                  )
    @click.option('-a', '--anatomy',
                  multiple=True,
                  type=(click.STRING, float),
                  help='The adjustment amount on given anatomical location in "um".',
                  )
    @click.option('-l', '--location',
                  type=click.STRING,
                  help='The location of the recording, i.e. "room1".',
                  )
    @click.option('--action-id',
                  type=click.STRING,
                  help=('Desired action id for this action, if none' +
                        ', it is generated from axona-path.'),
                  )
    @click.option('--no-local',
                  is_flag=True,
                  help='Store temporary on local drive.',
                  )
    @click.option('--no-files',
                  is_flag=True,
                  help='Generate action without storing files.',
                  )
    @click.option('--no-modules',
                  is_flag=True,
                  help='Generate action without storing modules.',
                  )
    @click.option('--subject-id',
                  type=click.STRING,
                  help='The id number of the subject.',
                  )
    @click.option('--overwrite',
                  is_flag=True,
                  help='Overwrite modules or not.',
                  )
    @click.option('-m', '--message',
                  multiple=True,
                  type=click.STRING,
                  help='Add message, use "text here" for sentences.',
                  )
    @click.option('-t', '--tag',
                  multiple=True,
                  type=click.STRING,
                  help='Add tags to action.',
                  )
    @click.option('--get_inp',
                  is_flag=True,
                  help='Use Axona input ".inp.',
                  )
    @click.option('-y', '--yes',
                  is_flag=True,
                  help='Yes to depth registering query.',
                  )
    def generate_axona_action(action_id, axona_filename, anatomy, user,
                              no_local, overwrite, no_files, no_modules,
                              subject_id, location, message, tag,
                              get_inp, yes):
        if not axona_filename.endswith('.set'):
            print("Sorry, we need an Axona .set file not " +
                  "'{}'.".format(axona_filename))
            return
        project = expipe.get_project(PAR.USER_PARAMS['project_id'])
        subject_id = subject_id or axona_filename.split(os.sep)[-2]
        axona_file = pyxona.File(axona_filename)
        if action_id is None:
            session_dtime = datetime.strftime(axona_file._start_datetime,
                                              '%d%m%y')
            basename, _ = op.splitext(axona_filename)
            session = basename[-2:]
            action_id = subject_id + '-' + session_dtime + '-' + session
        action = project.require_action(action_id)

        action.type = 'Recording'
        action.datetime = axona_file._start_datetime
        action.tags = list(tag) + ['axona']
        print('Registering action id ' + action_id)
        print('Registering subject id ' + subject_id)
        action.subjects = [subject_id]
        user = user or PAR.USER_PARAMS['user_name']
        if user is None:
            raise ValueError('Please add user name')
        if len(user) == 0:
            raise ValueError('Please add user name')
        print('Registering user ' + user)
        action.users = [user]
        location = location or PAR.USER_PARAMS['location']
        if location is None:
            raise ValueError('Please add location')
        if len(location) == 0:
            raise ValueError('Please add location')
        assert location in PAR.POSSIBLE_LOCATIONS
        print('Registering location ' + location)
        action.location = location
        action.messages = [{'message': m,
                             'user': user,
                             'datetime': datetime.now()}
                           for m in message]
        if not no_modules:
            action_tools.generate_templates(action, PAR.TEMPLATES['axona'], overwrite,
                                            git_note=action_tools.get_git_info())
            action_tools.register_depth(project, action, anatomy, yes=yes)

        if not no_files:
            fr = action.require_filerecord()
            if not no_local:
                exdir_path = action_tools._get_local_path(fr, make=False)
            else:
                exdir_path = fr.server_path
            if op.exists(exdir_path):
                if overwrite:
                    shutil.rmtree(exdir_path)
                else:
                    raise FileExistsError('The exdir path to this action "' +
                                          exdir_path + '" exists, use ' +
                                          'overwrite flag')
            else:
                os.makedirs(op.dirname(exdir_path))
            axona.convert(axona_file, exdir_path)
            axona.generate_tracking(exdir_path, axona_file)
            axona.generate_analog_signals(exdir_path, axona_file)
            axona.generate_spike_trains(exdir_path, axona_file)
            axona.generate_units(exdir_path, axona_file)
            if get_inp:
                axona.generate_inp(exdir_path, axona_file)
            else:
                warnings.warn('Not registering Axona ".inp".')
            axona.generate_clusters(exdir_path, axona_file)
        time_string = exdir.File(exdir_path).attrs['session_start_time']
        dtime = datetime.strptime(time_string, '%Y-%m-%dT%H:%M:%S')
        action.datetime = dtime

    # @cli.command('correct-date')
    # def correct_date():
    #     """Generate an axona recording-action to database.
    #
    #     COMMAND: axona-filename"""
    #     project = expipe.get_project(PAR.USER_PARAMS['project_id'])
    #
    #
    #     for action in project.actions:
    #         if action.type != 'Recording':
    #             continue
    #         if action.tags is None:
    #             print('No tags {}, {}'.format(action.id, action.tags))
    #             continue
            # if 'axona' not in action.tags:
            #     continue
            # if action.datetime is None:
            #     print('No datetime {}'.format(action.id))
            # local_path = '/media/norstore/server'
            # exdir_path = action.require_filerecord().exdir_path
            # exdir_path = op.join(local_path, exdir_path)
            # if not op.exists(exdir_path):
            #     print('Does not exist "' + exdir_path + '"')
            #     continue
            # exdir_file = exdir.File(exdir_path)
            # assert 'acquisition' in exdir_file, 'No acquisition {}'.format(action.id)
            # if 'axona_session' not in exdir_file['acquisition'].attrs:
            #     continue
            # session = exdir_file['acquisition'].attrs['axona_session']
            # axona_filename = op.join(exdir_path, 'acquisition', session,
            #                          session + '.set')
            # axona_file = pyxona.File(axona_filename)
            # # if action.datetime is not None:
            # #     assert action.datetime == axona_file._start_datetime, (
            # #         '{}, {}'.format(action.datetime,
            # #                         axona_file._start_datetime)
            # #     )
            # #     continue
            # print('Setting datetime on "' + action.id + '"')
            # action.datetime = axona_file._start_datetime