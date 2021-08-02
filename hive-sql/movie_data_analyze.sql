/**
 * 1）t_user观众表6000+数据
 * 字段列表：userid, sex, age, occupation, zipcode
 * 中文解释：用户id, 性别，年龄，职业，邮编
 * 2) t_movie表共3000+数据
 * 字段列表：movieid, moviename, movietype
 * 中文解释：电影id，电影名，电影类型
 * 3) t_rating表100万+条数据
 * 字段列表：userid, movieid, rate, times
 * 中文解释：用户id, 电影id，评分，评分时间 */

/* 1. 展示电影id为{2116}这部电影各年龄段的平均影评分 (reduce scan rows) */
select
    usr.age,
    avg(movie.rate) as avgrate
from (
    select
    rt.rate,
    rt.userid
    from hive_sql_test1.t_rating as rt where rt.movieid = 2116
) as movie left join hive_sql_test1.t_user as usr on movie.userid = usr.userid
group by usr.age;

/* 2. 找出男性评分最高且评分次数超过50次的10部电影，展示电影名、平均影评分和评分次数 */
select
    usr.sex,
    mov.moviename,
    avg(rt.rate) as avgrate,
    count(rt.movieid) as total
from hive_sql_test1.t_rating rt left join hive_sql_test1.t_user as usr on usr.userid = rt.userid
left join hive_sql_test1.t_movie as mov on mov.movieid = rt.movieid
where rt.movieid in
(select -- this is used to performance tuning, reduce scan rows
    rt.movieid
from hive_sql_test1.t_rating as rt
group by rt.movieid
having count(rt.movieid) > 50)
and usr.sex = 'M'
group by rt.movieid having count(rt.movieid) > 50
order by avgrate desc;