from expipe_plugin_cinpla.imports import *
from .config import load_parameters

nwb_main_groups = ['acquisition', 'analysis', 'processing', 'epochs',
                   'general']
tmp_phy_folders = ['.klustakwik2', '.phy', '.spikedetect']


def query_yes_no(question, default="yes", answer=None):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    if answer is not None:
        assert isinstance(answer, bool)
        return answer
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [[Y]/n] "
    elif default == "no":
        prompt = " [y/[N]] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def deltadate(adjustdate, regdate):
    delta = regdate - adjustdate if regdate > adjustdate else timedelta.max
    return delta


def position_to_dict(depth):
    position = {d[0]: dict() for d in depth}
    for key, num, val, unit in depth:
        probe_key = 'probe_{}'.format(num)
        position[key][probe_key] = pq.Quantity(val, unit)
    return position


def get_depth_from_surgery(project, entity_id):
    index = 0
    surgery = project.actions[entity_id + '-surgery-implantation']
    position = {}
    for key, module in surgery.modules.items():
        for probe_key, probe in module.items():
            if probe_key.startswith('probe_') and probe_key.split('_')[-1].isnumeric():
                if key not in position:
                    position[key] = {}
                position[key][probe_key] = probe['position']
    for key, groups in position.items():
        for group, pos in groups.items():
            if not isinstance(pos, pq.Quantity):
                raise ValueError('Depth of implant ' +
                                 '"{} {} = {}"'.format(key, group, pos) +
                                 ' not recognized')
            position[key][group] = pos.astype(float)[2]  # index 2 = z
    return position


def get_depth_from_adjustment(project, action, entity_id):
    DTIME_FORMAT = expipe.core.datetime_format
    try:
        adjustments = project.actions[entity_id + '-adjustment']
    except KeyError as e:
        return None, None
    adjusts = {}
    for adjust in adjustments.modules.values():
        values = adjust.contents
        adjusts[datetime.strptime(values['date'], DTIME_FORMAT)] = adjust

    regdate = action.datetime
    adjustdates = adjusts.keys()
    adjustdate = min(adjustdates, key=lambda x: deltadate(x, regdate))
    return adjusts[adjustdate]['depth'].contents, adjustdate


def register_depth(project, action, depth=None, answer=None):
    if len(action.entities) != 1:
        print('Exactly 1 entity is required to register depth.')
        return False
    depth = depth or []
    curr_depth = None
    if len(depth) > 0:
        curr_depth = position_to_dict(depth)
        adjustdate = None
    else:
        curr_depth, adjustdate = get_depth_from_adjustment(
            project, action, action.entities[0])
    if curr_depth is None:
        print('Cannot find current depth from adjustments.')
        return False

    def last_num(x):
        return '{:03d}'.format(int(x.split('_')[-1]))
    print('Adjust date time: {}\n'.format(adjustdate))
    print(''.join('Depth: {} {} = {}\n'.format(key, probe_key, val[probe_key])
            for key, val in curr_depth.items()
            for probe_key in sorted(val, key=lambda x: last_num(x))))
    correct = query_yes_no(
        'Are the values correct?',
        answer=answer)
    if not correct:
        return False

    action.create_module(name='depth', contents=curr_depth)
    return True


def _make_data_path(action, overwrite):
    action_path = action._backend.path
    project_path = action_path.parent.parent
    data_path = action_path / 'data'
    data_path.mkdir(exist_ok=True)
    exdir_path = data_path / 'main.exdir'
    if exdir_path.exists():
        if overwrite:
            shutil.rmtree(str(exdir_path))
        else:
            raise FileExistsError(
                'The exdir path to this action "' + str(exdir_path) +
                '" exists, optionally use "--overwrite"')
    relpath = exdir_path.relative_to(project_path)
    action.data['main'] = str(relpath)
    return exdir_path


def _get_data_path(action):
    action_path = action._backend.path
    project_path = action_path.parent.parent
    data_path = str(pathlib.Path(pathlib.PureWindowsPath(action.data['main'])))
    return project_path / data_path


def register_templates(action, templates, overwrite=False):
    '''
    Parameters
    ----------
    action : expipe.Action
    templates : list
    '''
    for template in templates:
        try:
            action.create_module(template=template)
            print('Adding module ' + template)
        except KeyError as e:
            if overwrite:
                action.delete_module(template)
                action.create_module(template=template)
                print('Adding module ' + template)
            else:
                raise KeyError(str(e) + '. Optionally use "overwrite"')
        except Exception as e:
            print(template)
            raise e
