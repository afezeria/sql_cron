-- author afezeria
--  文件格式說明
--  ┌──分鐘（0 - 59）
--  │ ┌──小時（0 - 23）
--  │ │ ┌──日（1 - 31）
--  │ │ │ ┌─月（1 - 12）
--  │ │ │ │ ┌─星期（0 - 6，表示从周日到周六）
--  │ │ │ │ │
--  *  *  *  *  *
-- 支持的符号： , - / ?(日和星期)
-- 不支持同时出现多个符号
create or replace function hymn.get_next_execution_time(cron text, last_exec_time timestamptz) returns timestamptz
    language plpgsql
    immutable as
$$
declare
    cron_item      text[]      := regexp_split_to_array(cron, '\s+');
    now            timestamptz := now();
--     上一次
    l_month        int         := extract(month from last_exec_time);
    l_day          int         := extract(day from last_exec_time);
    l_hour         int         := extract(hour from last_exec_time);
    l_minute       int         := extract(minute from last_exec_time);
--     可用的
    a_week_day_arr int[];
    a_month_arr    int[];
    a_day_arr      int[];
    a_hour_arr     int[];
    a_minute_arr   int[];
--     新的
--     0-6
    n_week_day     int;
--     1-12
    n_month        int;
--     1-31
    n_day          int;
--     0-23
    n_hour         int;
--     0-59
    n_minute       int;
    n_year         int         := extract(year from last_exec_time);
--     表达式
    expr_dow       text;
    expr_month     text;
    expr_day       text;
    expr_hour      text;
    expr_minute    text;
    n_timestamptz  timestamptz;
--     为true表示忽略dayOfWeek(不判断dayOfWeek）,为false表示忽略day（此时day下界永远为1）
    ignore_dow     bool;
    loop_count     int         := 0;
begin
    if array_length(cron_item, 1) <> 5 then
        raise exception 'invalid cron expression, expression can only 5 item';
    end if;
    expr_dow = cron_item[5];
    expr_month = cron_item[4];
    expr_day = cron_item[3];
    expr_hour = cron_item[2];
    expr_minute = cron_item[1];

    if (expr_day = '*' and expr_dow = '*')
        or (expr_day = '*' and expr_dow = '?')
        or (expr_day != '*' and expr_day != '?' and expr_dow = '?') then
        ignore_dow = true;
    elsif (expr_day = '?' and expr_dow != '*' and expr_dow != '?') then
        ignore_dow = false;
    else
        raise exception 'invalid cron expression, day and day_of_week cannot be meaningful at the same time';
    end if;
    <<l1>>
    loop
        loop_count = loop_count + 1;
        if loop_count > 100 then
            raise exception '无法找到下一次执行时间';
        end if;
        a_week_day_arr = hymn.filter_arr_in_range(
                hymn.parse_cron_sub_expr_and_get_range('day_of_week', expr_dow, 0, 6),
                null, null
            );
        raise notice 'week %',a_week_day_arr;
        a_month_arr = hymn.filter_arr_in_range(
                hymn.parse_cron_sub_expr_and_get_range('month', expr_month, 1, 12),
                l_month, null
            );
        if array_length(a_month_arr, 1) is null then
--         没有可用月份时年分加一，
            n_year = n_year + 1;
--             年份更新后月份从头开始
            l_month = 1;
--             月重新计算后天数从头开始
            l_day = 1;
            l_hour = 0;
            l_minute = 0;
            continue;
        end if;
        a_day_arr = hymn.filter_arr_in_range(
                hymn.parse_cron_sub_expr_and_get_range('day', expr_day, 1, 31),
                l_day,
--             取当前月份
                extract(days FROM date_trunc('month', make_date(n_year, a_month_arr[0], 1)) +
                                  interval '1 month - 1 day')::int
            );
        if array_length(a_day_arr, 1) is null then
            l_month = l_month + 1;
            l_day = 1;
            l_hour = 0;
            l_minute = 0;
            continue;
        end if;

        a_hour_arr = hymn.filter_arr_in_range(
                hymn.parse_cron_sub_expr_and_get_range('hour', expr_hour, 0, 23),
                l_hour, null
            );
        if array_length(a_hour_arr, 1) is null then
            l_day = l_day + 1;
            l_hour = 0;
            l_minute = 0;
            continue;
        end if;
        a_minute_arr = hymn.filter_arr_in_range(
                hymn.parse_cron_sub_expr_and_get_range('minute', expr_minute, 0, 59),
                l_minute, null
            );
        if array_length(a_minute_arr, 1) is null then
            l_hour = l_hour + 1;
            l_minute = 0;
            continue;
        end if;
        n_week_day = a_week_day_arr[1];
        n_month = a_month_arr[1];
        n_day = a_day_arr[1];
        n_hour = a_hour_arr[1];

        foreach n_minute in array a_minute_arr
            loop
                n_timestamptz =
                        make_timestamptz(n_year, n_month, n_day, n_hour, n_minute, 0);

                if not ignore_dow then
                    if array_position(a_week_day_arr,
                                      extract(dow from n_timestamptz)::int) is null then
                        l_day = l_day + 1;
                        l_hour = 0;
                        l_minute = 0;
                        continue l1;
                    end if;
                end if;
                if n_timestamptz > last_exec_time then
                    return n_timestamptz;
                end if;
            end loop;
    end loop;
end;
$$;

comment on function hymn.get_next_execution_time(text, timestamptz) is '输入cron表达式和上一次执行时间返回下一次执行时间';


create or replace function hymn.filter_arr_in_range(source int[], min int, max int) returns int[]
    language plpgsql as
$$
declare
    arr int[];
begin
    if min is not null then
        if max is not null then
            select array(select i from unnest(source) as t(i) where i between min and max) into arr;
        else
            select array(select i from unnest(source) as t(i) where i >= min) into arr;
        end if;
    else
        if max is not null then
            select array(select i from unnest(source) as t(i) where i <= max) into arr;
        else
            arr = source;
        end if;
    end if;
    return arr;
end;
$$;
comment on function hymn.filter_arr_in_range(int[], int, int) is '根据区间上界和下界过滤int数组，上界和下界为空';


create or replace function hymn.parse_cron_sub_expr_and_get_range(d_name text, expr text, lp int, rp int) returns int[]
    language plpgsql
    immutable as
$$
declare
    ia      int[];
--     range/enum/step
    e_type  text;
    r_left  int;
    r_right int;
begin
    if regexp_match(expr,
                    '^(\?|\*|\d{1,2}-\d{1,2}|\d{1,2}/\d{1,2}|\d{1,2}(,\d{1,2})?)$') is null then
        raise exception 'invalid % group: %',d_name,expr;
    end if;
    if length(expr) = 1 then
        if expr = '*' then
            return ARRAY(SELECT * FROM generate_series(lp, rp));
        elsif expr = '?' then
            if d_name != 'day' and d_name != 'day_of_week' then
                raise exception 'only day and day_of_week group support ''?''';
            else
                return ARRAY(SELECT * FROM generate_series(lp, rp));
            end if;
        else
            r_left = expr::int;
            if r_left < lp then
                raise exception 'invalid % group, % number must be between % and %',d_name,d_name,lp,rp;
            end if;
            return array [expr::int];
        end if;
    elsif length(expr) = 2 then
        r_left = expr::int;
        if r_left < lp or r_left > rp then
            raise exception 'invalid % group, % number must be between % and %',d_name,d_name,lp,rp;
        end if;
        return array [expr::int];
    end if;
--     如果expr中包含 - 就是区间型，不包含是枚举型
    if position('-' in expr) <> 0 then
        e_type = 'range';
    elsif position('/' in expr) <> 0 then
        e_type = 'step';
    else
        e_type = 'enum';
    end if;
    if e_type = 'range' then
        r_left = (regexp_split_to_array(expr, '-'))[1]::int;
        r_right = (regexp_split_to_array(expr, '-'))[2]::int;
        if r_left < lp or r_left > rp or r_right < lp or r_right > rp then
            raise exception 'invalid % group, % number must be between % and %',d_name,d_name,lp,rp;
        end if;
        if r_right <= r_left then
            raise exception 'invalid % group, right endpoint must be greater than left endpoint',d_name;
        end if;
        ia = ARRAY(SELECT * FROM generate_series(r_left, r_right));
    elsif e_type = 'step' then
--         起始值
        r_left = (regexp_split_to_array(expr, '/'))[1]::int;
--         步长
        r_right = (regexp_split_to_array(expr, '/'))[2]::int;
        if r_left < lp or r_left > rp then
            raise exception 'invalid % group, start value must be between % and %',d_name,lp,rp;
        end if;
        if r_right < 2 or r_right > rp then
            raise exception 'invalid % group, step size must be between 2 and %',d_name,rp - 1;
        end if;
        ia = array [r_left];
        loop
            r_left = r_left + r_right;
            exit when r_left > rp;
            ia = array_append(ia, r_left);
        end loop;
    else
        select array_agg(x)
        into ia
        from (SELECT unnest(regexp_split_to_array(expr, ','))::int AS x
              ORDER BY x) a;
        if ia[1] < lp or ia[array_upper(ia, 1)] > rp then
            raise exception 'invalid % group, start value must be between % and %',d_name,lp,rp;
        end if;
    end if;
    return ia;
end;
$$;