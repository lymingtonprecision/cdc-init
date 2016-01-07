begin
  ? := ifsapp.lpe_change_data_capture_api.create_seed_view(
    :schema,
    :table,
    null,
    :alias
  );
end;
