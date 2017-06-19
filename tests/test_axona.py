import pytest
import expipe
import subprocess
import click
from click.testing import CliRunner
import quantities as pq
import os.path as op

expipe.ensure_testing()


@click.group()
@click.pass_context
def cli(ctx):
    pass

from expipe_plugin_cinpla.axona import AxonaPlugin
AxonaPlugin().attach_to_cli(cli)

from expipe_plugin_cinpla.main import CinplaPlugin
CinplaPlugin().attach_to_cli(cli)


def run_command(command_list, inp=None):
    result = CliRunner().invoke(cli, command_list, input=inp)
    if result.exit_code != 0:
        print(result.output)
        raise result.exception
    return result


def test_axona(teardown_setup_project):
    project, _ = teardown_setup_project
    # make surgery action
    run_command(['register-surgery', pytest.RAT_ID,
                 '--weight', '500',
                 '--birthday', '21.05.2017',
                 '--procedure', 'implantation',
                 '-d', '21.01.2017T14:40',
                 '-a', 'mecl', 1.9,
                 '-a', 'mecr', 1.8])

    # init adjusment
    run_command(['adjust', pytest.RAT_ID,
                 '-a', 'mecl', 50,
                 '-a', 'mecr', 50,
                 '-d', 'now',
                 '--init'], inp='y')

    data_path = op.join(expipe.settings['data_path'],
                        pytest.USER_PAR.project_id,
                        pytest.RAT_ID + '-311013-03')
    if op.exists(data_path):
        import shutil
        shutil.rmtree(data_path)
    currdir = op.abspath(op.dirname(__file__))
    axona_filename = op.join(currdir, 'test_data', 'axona', 'DVH_2013103103.set')
    res = run_command(['register-axona', axona_filename,
                         '--subject-id', pytest.RAT_ID,
                         '-m', 'some message',
                         '-t', pytest.POSSIBLE_TAGS[0]], inp='y')
