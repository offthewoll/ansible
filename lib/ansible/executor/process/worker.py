# (c) 2012-2014, Michael DeHaan <michael.dehaan@gmail.com>
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

# Make coding more python3-ish
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import multiprocessing
import os
import sys
import traceback

from jinja2.exceptions import TemplateNotFound

# TODO: not needed if we use the cryptography library with its default RNG
# engine
HAS_ATFORK=True
try:
    from Crypto.Random import atfork
except ImportError:
    HAS_ATFORK=False

from ansible.errors import AnsibleConnectionFailure
from ansible.executor.task_executor import TaskExecutor
from ansible.executor.task_result import TaskResult
from ansible.module_utils._text import to_text

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()

__all__ = ['WorkerProcess']


class WorkerProcess(multiprocessing.Process):
    '''
    The worker thread class, which uses TaskExecutor to run tasks
    read from a job queue and pushes results into a results queue
    for reading later.
    '''

    def __init__(self, rslt_q, play, host, task, task_vars, play_context, loader, variable_manager, shared_loader_obj):

        print(u"WORKER STARTING INIT: %s - %s" % (to_text(host), to_text(task)))
        super(WorkerProcess, self).__init__()
        # takes a task queue manager as the sole param:
        self._rslt_q            = rslt_q
        self._play              = play
        self._host              = host
        self._task              = task
        self._play_context      = play_context
        self._loader            = loader
        self._variable_manager  = variable_manager
        self._shared_loader_obj = shared_loader_obj

        self._task_vars = task_vars

        # dupe stdin, if we have one
        self._new_stdin = sys.stdin
        try:
            fileno = sys.stdin.fileno()
            if fileno is not None:
                try:
                    self._new_stdin = os.fdopen(os.dup(fileno))
                except OSError:
                    # couldn't dupe stdin, most likely because it's
                    # not a valid file descriptor, so we just rely on
                    # using the one that was passed in
                    pass
        except (AttributeError, ValueError):
            # couldn't get stdin's fileno, so we just carry on
            pass
        print(u"WORKER DONE WITH INIT: %s - %s" % (to_text(host), to_text(task)))

    def start(self, tqm):
        print(u"WORKER CALLING START: %s - %s" % (to_text(self._host), to_text(self._task)))
        try:
            tqm._queued_tasks_lock.release()
        except:
            pass
        super(WorkerProcess, self).start()

    def run(self):
        '''
        Called when the process is started.  Pushes the result onto the
        results queue. We also remove the host from the blocked hosts list, to
        signify that they are ready for their next task.
        '''

        print(u"WORKER STARTING RUN: %s - %s" % (to_text(self._host), to_text(self._task)))
        #import cProfile, pstats, StringIO
        #pr = cProfile.Profile()
        #pr.enable()

        if HAS_ATFORK:
            atfork()

        try:
            # execute the task and build a TaskResult from the result
            display.debug("running TaskExecutor() for %s/%s" % (self._host, self._task))
            print(u"WORKER RUNNING TASK: %s - %s" % (to_text(self._host), to_text(self._task)))
            executor_result = TaskExecutor(
                self._host,
                self._task,
                self._task_vars,
                self._play_context,
                self._new_stdin,
                self._loader,
                self._shared_loader_obj,
                self._rslt_q
            ).run()
            print(u"WORKER DONE RUNNING TASK: %s - %s" % (to_text(self._host), to_text(self._task)))

            display.debug("done running TaskExecutor() for %s/%s" % (self._host, self._task))
            self._host.vars = dict()
            self._host.groups = []
            task_result = TaskResult(self._host.name, self._task._uuid, executor_result)

            # put the result on the result queue
            display.debug("sending task result")
            self._rslt_q.put(task_result)
            display.debug("done sending task result")

        except AnsibleConnectionFailure:
            self._host.vars = dict()
            self._host.groups = []
            task_result = TaskResult(self._host.name, self._task._uuid, dict(unreachable=True))
            self._rslt_q.put(task_result, block=False)

        except Exception as e:
            print(u"WORKER EXCEPTION: %s" % to_text(e))
            print(u"WORKER TRACEBACK: %s" % to_text(traceback.format_exc()))
            if not isinstance(e, (IOError, EOFError, KeyboardInterrupt, SystemExit)) or isinstance(e, TemplateNotFound):
                try:
                    self._host.vars = dict()
                    self._host.groups = []
                    task_result = TaskResult(self._host.name, self._task._uuid, dict(failed=True, exception=to_text(traceback.format_exc()), stdout=''))
                    self._rslt_q.put(task_result, block=False)
                except:
                    display.debug(u"WORKER EXCEPTION: %s" % to_text(e))
                    display.debug(u"WORKER TRACEBACK: %s" % to_text(traceback.format_exc()))

        display.debug("WORKER PROCESS EXITING")

        #pr.disable()
        #s = StringIO.StringIO()
        #sortby = 'time'
        #ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        #ps.print_stats()
        #with open('worker_%06d.stats' % os.getpid(), 'w') as f:
        #    f.write(s.getvalue())

        print(u"WORKER DONE: %s - %s" % (to_text(self._host), to_text(self._task)))
        sys.exit(0)
