% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_workers).

-export([
   worker_register/4,
   worker_unregister/1,

   membership_subscribe/3,
   membership_unsubscribe/1,

   get_workers/1
]).


-callback couch_workers_membership_update(
    WorkerType :: term(),
    Workers :: #{},
    VStamp :: binary(),
    SubscriberRef :: reference()
) -> ok.


% External API

worker_register(WorkerType, Id, Opts, Pid)  when is_binary(Id), is_map(Opts),
        is_pid(Pid) ->
    couch_workers_local:worker_register(WorkerType, Id, Pid).


worker_unregister(Ref) when is_reference(Ref) ->
    couch_workers_local:worker_unregister(Ref).


membership_subscribe(WorkerType, Module, Pid) ->
    couch_workers_global:subscribe(WorkerType, Module, Pid).


membership_unsubscribe(Ref) when is_reference(Ref) ->
    couch_workers_global:unsubscribe(Ref).


get_workers(WorkerType) ->
    couch_workers_global:get_workers(WorkerType).
