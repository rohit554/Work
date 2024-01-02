insert overwrite dgdm_{tenant}.dim_users
select distinct
    u.id as userId,
    u.username as userName,
    u.name as userFullName,
    u.email as userEmail,
    u.title as userTitle,
    u.department,
    u.manager.id as managerId,
    u.employerInfo.dateHire,
    u.state
from gpc_{tenant}.raw_users u
left join
    gpc_{tenant}.raw_users m
on u.manager.id = m.id
