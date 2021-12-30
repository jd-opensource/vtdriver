create user 'vtdriver' @'%' identified by 'vtdriver_password';
grant all privileges on *.* to 'vtdriver' @'%';
flush privileges;
