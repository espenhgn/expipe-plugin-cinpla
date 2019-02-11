from expipe_plugin_cinpla.imports import *
from expipe_plugin_cinpla.scripts.utils import (
    register_templates, query_yes_no)
from . import utils

def attach_to_cli(cli):
    @cli.command('annotate', short_help='Parse info about recorded units')
    @click.argument('action-id', type=click.STRING)
    @click.option('-t', '--tag',
                    multiple=True,
                    type=click.STRING,
                    callback=utils.optional_choice,
                    envvar=PAR.POSSIBLE_TAGS,
                    help='Add tags to action.',
                    )
    @click.option('--message', '-m',
                  type=click.STRING,
                  help='Add message, use "text here" for sentences.',
                  )
    @click.option('-u', '--user',
                  type=click.STRING,
                  help='The experimenter performing the annotation.',
                  )
    def annotate(action_id, tag, message, user):
        project = PAR.PROJECT
        action = project.actions[action_id]
        user = user or PAR.USERNAME
        if user is None:
            raise ValueError('Please add user name')
        users = list(set(action.users))
        if user not in users:
            users.append(user)
        action.users = users
        if message:
            action.create_message(text=m, user=user, datetime=datetime.now())
        action.tags.extend(tag)

    @cli.command('spikesort', short_help='Spikesort with klustakwik.')
    @click.argument('action-id', type=click.STRING)
    @click.option('--no-local',
                  is_flag=True,
                  help='Store temporary on local drive.',
                  )
    def spikesort(action_id, no_local):
        # anoying!!!!
        import logging
        from phycontrib.neo.model import NeoModel
        logger = logging.getLogger('phy')
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)

        project = PAR.PROJECT
        action = project.require_action(action_id)
        exdir_path = PAR.PROJECT_ROOT / action.data[0]
        print('Spikesorting ', exdir_path)
        model = NeoModel(exdir_path)
        channel_groups = model.channel_groups
        for channel_group in channel_groups:
            if not channel_group == model.channel_group:
                model.load_data(channel_group)
            print('Sorting channel group {}'.format(channel_group))
            clusters = model.cluster(np.arange(model.n_spikes), model.channel_ids)
            model.save(spike_clusters=clusters)

    @cli.command('add-server')
    @click.option(
        '--name', '-n', type=click.STRING, help='The name of the server'
    )
    @click.option(
        '--domain', '-d', type=click.STRING, help='The server domain (or IP address)'
    )
    @click.option(
        '--username', '-un', type=click.STRING, help='The username to access the server.'
    )
    @click.option(
        '--password', '-pw', type=click.STRING, prompt=True, hide_input=True, help='The server password (will be prompted)'
    )
    def add_server(name, domain, username, password):
        """Add server info."""
        cwd = pathlib.Path.cwd()
        local_root, _ = expipe.config._load_local_config(cwd)
        path = None
        config = expipe.config._load_config_by_name(path)
        current_servers = config.get('servers') or []
        if name is not None and domain is not None and username is not None and password is not None:
            pass
        else:
            raise Exception("Provide server name, domain, and username. Password is prompted.")
        new_server = {'host': name, 'domain': domain, 'user': username, 'password': password}
        current_servers.append(new_server)
        config['servers'] = current_servers
        expipe.config._dump_config_by_name(path, config)
