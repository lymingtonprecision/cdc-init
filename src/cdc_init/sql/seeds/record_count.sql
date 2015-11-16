select
  ifsapp.lpe_change_data_capture_api.count_seed_records(
    :schema,
    :table
  ) n
from dual
