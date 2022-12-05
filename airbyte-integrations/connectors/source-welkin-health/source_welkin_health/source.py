#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.sources.streams import IncrementalMixin


"""
Add integration tests and unit tests.
Open-source this connector.
"""

class BaseWelkinHealthStream(HttpStream, ABC):
    def __init__(self, tenant_name: str, instance_name : str, start_date: str, **kwargs):
        super().__init__(**kwargs)
        self._start_date = start_date
        self._tenant_name = tenant_name
        self._instance_name = instance_name
        self._cursor_value = None


# Basic full refresh stream
class WelkinHealthStream(BaseWelkinHealthStream, ABC):
    primary_key = "id"
    page_size = 100

    @property
    def url_base(self) -> str:
        return f"https://api.live.welkincloud.io/{self._tenant_name}/{self._instance_name}/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

# Basic incremental stream
class IncrementalWelkinHealthStream(WelkinHealthStream, IncrementalMixin, ABC):

    state_checkpoint_interval = 500
    
    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: self.start_date}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
       self._cursor_value = value[self.cursor_field]

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        latest_state = latest_record.get(self.cursor_field)
        current_state = current_stream_state.get(self.cursor_field) or latest_state
        if latest_state:
            return {self.cursor_field: max(latest_state, current_state)}
        else:
            return {self.cursor_field: current_state}

class ExportIncrementalWelkinHealthStream(IncrementalWelkinHealthStream, ABC):
    primary_key = "id"
    cursor_field = "updatedAt"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        try:
            if response.json()['cursor']['hasNext']:
                return {"start": response.json()['cursor']['end']}
            else:
                return None
        except:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if next_page_token is not None:
            return {"start": next_page_token['start']}
        elif self._cursor_value:
            return {"start": self._cursor_value}
        else:
            return {"start": self._start_date}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()['data']
        yield from [record for record in data]

class Assessments(ExportIncrementalWelkinHealthStream):
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/ASSESSMENT"

class CalendarEvents(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/CALENDAR_EVENT"

class CdtRecords(ExportIncrementalWelkinHealthStream):
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/CDT_RECORD"

class Emails(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/EMAIL"

class Encounters(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/ENCOUNTER"

class EncounterComments(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/ENCOUNTER_COMMENT"

class EncounterDispositions(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/ENCOUNTER_DISPOSITION"

class Patients(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/PATIENT"

class Sms(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/SMS"


class Tasks(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/TASK"

class VoiceCalls(ExportIncrementalWelkinHealthStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "export/VOICE_CALL"

class ReportPatientProgramPhaseDistribution(WelkinHealthStream):
    primary_key = "title"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "reports/patient-program-phase-distribution"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        yield from [record for record in data]

# Source
class SourceWelkinHealth(AbstractSource):

    @classmethod
    def get_authenticator(cls, config: Mapping[str, Any]) -> TokenAuthenticator:
        tenant_name = config.get("tenant_name")
        client_name = config.get("client_name")
        client_secret = config.get("client_secret")
        
        #to open source, erase what's below
        dreem_login = config.get("dreem_login")
        dreem_password = config.get("dreem_password")
        url = "https://login.rythm.co/welkin_token/token/"
        r = requests.request(
                "GET",
                url,
                auth=(dreem_login, dreem_password)
            )
        #to open source, uncomment the line below    
        #r = requests.post(f"https://api.live.welkincloud.io/{tenant_name}/admin/api_clients/{client_name}", json={"secret":client_secret})
        r.raise_for_status()
        tok = r.json()["token"]
        return TokenAuthenticator(token=tok)

    @classmethod
    def convert_config2stream_args(cls, config: Mapping[str, Any]) -> Mapping[str, Any]:
        """Convert input configs to parameters of the future streams
        This function is used by unit tests too
        """
        return {
            "start_date": config["start_date"],
            "authenticator": cls.get_authenticator(config),
            "tenant_name": config["tenant_name"],
            "instance_name": config["instance_name"],
        }

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            args = self.convert_config2stream_args(config)
            stream = Patients(**args)
            records = stream.read_records(sync_mode=SyncMode.full_refresh)
            next(records)
            return True, None
        except Exception as e:
            return False, e


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Returns relevant a list of available streams
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        args = self.convert_config2stream_args(config)
        all_streams_mapping = {
            # sorted in alphabet order
            "assessments": Assessments(**args),
            "calendar_events": CalendarEvents(**args),
            "cdt_records": CdtRecords(**args),
            "emails": Emails(**args),
            "encounters": Encounters(**args),
            "encounter_comments": EncounterComments(**args),
            "encounter_dispositions": EncounterDispositions(**args),
            "patients": Patients(**args),
            "sms": Sms(**args),
            "tasks": Tasks(**args),
            "voice_calls": VoiceCalls(**args),
            "report_patient_program_phase_distribution": ReportPatientProgramPhaseDistribution(**args)
        }
        return [stream_cls for stream_cls in all_streams_mapping.values()]