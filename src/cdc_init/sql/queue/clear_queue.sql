begin
  execute immediate 'delete from ' || :table || ' where q_name = ''' || :queue || '''';
end;
