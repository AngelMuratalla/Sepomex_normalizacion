SET NAMES 'utf8';
DROP DATABASE IF EXISTS sepomex_mega;

CREATE DATABASE IF NOT EXISTS sepomex_mega DEFAULT CHARACTER SET 'utf8';
USE sepomex_mega;
CREATE TABLE sepomex(
	
	d_codigo				VARCHAR(45) NULL COMMENT '',
	d_asenta				VARCHAR(145) NULL COMMENT '',
	d_tipo_asenta			VARCHAR(45) NULL COMMENT '',
	d_mnpio					VARCHAR(45) NULL COMMENT '',
	d_estado				VARCHAR(45) NULL COMMENT '',
	d_ciudad				VARCHAR(45) NULL COMMENT '',
	d_cp					VARCHAR(45) NULL COMMENT '',
	c_estado				VARCHAR(7) NULL COMMENT '',
	c_oficina				VARCHAR(4) NULL COMMENT '',
	c_cp					VARCHAR(4) NULL COMMENT '',
	c_tipo_asenta			VARCHAR(10) NULL COMMENT '',
	c_mnpio					VARCHAR(4) NULL COMMENT '',
	id_asenta_cpcons		VARCHAR(5) NULL COMMENT '',
	d_zona					VARCHAR(45) NULL COMMENT '',
	c_cve_ciudad			VARCHAR(3) NULL COMMENT ''
	

)DEFAULT CHARACTER SET utf8;


LOAD DATA LOCAL infile "Codigos/CPdescarga2.txt" INTO TABLE sepomex
CHARACTER SET UTF8 fields terminated BY '|' lines terminated BY '\r\n';
