DELIMITER $$

DROP PROCEDURE IF EXISTS migrar_datos;
CREATE PROCEDURE migrar_datos()

BEGIN

	DECLARE f_finished INTEGER DEFAULT 0;
	
	DECLARE n_estado VARCHAR(45);
	DECLARE n_ciudad VARCHAR(45);
	DECLARE n_tipo VARCHAR(45);
	DECLARE n_mnpio VARCHAR(45);
	DECLARE	copo VARCHAR(45);
	DECLARE n_asenta VARCHAR(145);
	DECLARE id_tipo TINYINT;
	DECLARE id_mnpio SMALLINT;
	DECLARE id_ciudad SMALLINT;
	DECLARE id_estado TINYINT;
	DECLARE n_muni_edo INTEGER;
	DECLARE cnt INTEGER DEFAULT 0;
	
	DECLARE puntero_ets CURSOR FOR SELECT DISTINCT(d_estado) FROM sepomex_mega.sepomex;
	DECLARE puntero_cus CURSOR FOR SELECT DISTINCT(d_ciudad) FROM sepomex_mega.sepomex;
	DECLARE puntero_tps CURSOR FOR SELECT DISTINCT (d_tipo_asenta) FROM sepomex_mega.sepomex;
	DECLARE puntero_mns CURSOR FOR SELECT DISTINCT (d_mnpio) FROM sepomex_mega.sepomex;
	DECLARE puntero_ass CURSOR FOR SELECT d_asenta FROM sepomex_mega.sepomex;
	DECLARE puntero_cp CURSOR FOR SELECT d_codigo FROM sepomex_mega.sepomex;
	DECLARE puntero_cus_ass CURSOR FOR SELECT d_ciudad FROM sepomex_mega.sepomex;
	DECLARE puntero_tps_ass CURSOR FOR SELECT d_tipo_asenta FROM sepomex_mega.sepomex;
	DECLARE puntero_mns_ass CURSOR FOR SELECT d_mnpio FROM sepomex_mega.sepomex;
	DECLARE puntero_ets_ass CURSOR FOR SELECT d_estado FROM sepomex_mega.sepomex;

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET f_finished = 1;

	OPEN puntero_ets;
       	loop_ets: LOOP
		
			FETCH puntero_ets INTO n_estado;
	   
			IF f_finished = 1 THEN 
				SELECT 'Migracion de estados finalizada' as mensaje;
              	LEAVE loop_ets;
			END IF;
	
			INSERT INTO estados (nombre) VALUES (UPPER(n_estado));
           
       	END LOOP loop_ets;
    CLOSE puntero_ets;
	
	
	SET f_finished = 0;

	OPEN puntero_cus;
		loop_cus: LOOP
		
			FETCH puntero_cus INTO n_ciudad;

			IF f_finished = 1 THEN 
				SELECT 'Migracion de ciudades finalizada' as mensaje;
				LEAVE loop_cus;
			END IF;
			
			IF n_ciudad <> '' THEN	
				INSERT INTO ciudades (nombre) VALUES (UPPER(n_ciudad));
			END IF;
			
		END LOOP loop_cus;
	CLOSE puntero_cus;
	
	
	SET f_finished = 0;

	OPEN puntero_tps;
		loop_tps: LOOP
		
			FETCH puntero_tps INTO n_tipo;

			IF f_finished = 1 THEN 
				SELECT 'Migracion de tipos finalizada' as mensaje;
				LEAVE loop_tps;
			END IF;
			
			INSERT INTO tipos (nombre) VALUES (UPPER(n_tipo));

		END LOOP loop_tps;
	CLOSE puntero_tps;
	
	SET f_finished = 0;
	
	
	OPEN puntero_mns;
		loop_mns: LOOP
		
			FETCH puntero_mns INTO n_mnpio;
			
			/* Las siguientes dos lineas ayudan a saber si existe un municipio con el mismo nombre en dos estados diferentes */
			CREATE TABLE muni_estado SELECT DISTINCT d_mnpio , d_estado FROM sepomex_mega.sepomex WHERE d_mnpio = n_mnpio;
			SET n_muni_edo = (SELECT COUNT(*) FROM muni_estado);
			
			IF f_finished = 1 THEN 
				DROP TABLE IF EXISTS muni_estado;
				SELECT 'Migracion de municipios finalizada' as mensaje;
				LEAVE loop_mns;
			END IF;
			
			IF n_muni_edo = 1 THEN
			
				SET n_estado = (SELECT DISTINCT d_estado FROM sepomex_mega.sepomex WHERE d_mnpio = n_mnpio);
				SET id_estado = (SELECT id_ets FROM estados where nombre = UPPER(n_estado));
				INSERT INTO municipios (nombre , id_ets) VALUES (UPPER(n_mnpio), id_estado);
				
			ELSE
			
				SET cnt = 0;
				WHILE cnt < n_muni_edo DO
						
					
					SET n_estado = (SELECT d_estado FROM muni_estado LIMIT 1);
					SET id_estado = (SELECT id_ets FROM estados where nombre = UPPER(n_estado));
					
					INSERT INTO municipios (nombre , id_ets) VALUES (UPPER(n_mnpio), id_estado);
					DELETE FROM muni_estado LIMIT 1;
					
					SET cnt = cnt + 1; 
					
				END WHILE;
			END IF;
			
			DROP TABLE IF EXISTS muni_estado;
		END LOOP loop_mns;
	CLOSE puntero_mns;
	
	SET f_finished = 0;

	OPEN puntero_cp;
	OPEN puntero_ass;
	OPEN puntero_tps_ass;
	OPEN puntero_mns_ass;
	OPEN puntero_cus_ass;
	OPEN puntero_ets_ass;
		loop_cp: LOOP
		
		
			FETCH puntero_cp INTO copo;
			FETCH puntero_ass INTO n_asenta;
			FETCH puntero_tps_ass INTO n_tipo;
			FETCH puntero_mns_ass INTO n_mnpio;
			FETCH puntero_cus_ass INTO n_ciudad;
			FETCH puntero_ets_ass INTO n_estado;

			SET id_tipo = (SELECT id_tps FROM tipos where nombre = UPPER(n_tipo));
			
			/* Usamos el id_estado para asignar el municipio correcto */
			SET id_estado = (SELECT id_ets FROM estados WHERE nombre = UPPER(n_estado));
			SET id_mnpio = (SELECT id_mns FROM municipios WHERE nombre = UPPER(n_mnpio) and id_ets = id_estado);
			
			/* El siguiente if inserta un NULL en los registros sin ciudad */ 
			IF n_ciudad = '' THEN
				SET id_ciudad = NULL;
			ELSE
				SET id_ciudad = (SELECT id_cus FROM ciudades where nombre = UPPER(n_ciudad));
			END IF;
	
			IF f_finished = 1 THEN 
				SELECT 'Migracion de asentamientos finalizada' as mensaje;
				LEAVE loop_cp;
			END IF;
			INSERT IGNORE INTO asentamientos(cp,nombre,id_tps,id_mns,id_cus) 
			VALUES (copo, UPPER(n_asenta), id_tipo , id_mnpio , id_ciudad);

		END LOOP loop_cp;
	CLOSE puntero_cp;
	CLOSE puntero_ass;
	CLOSE puntero_tps_ass;
	CLOSE puntero_mns_ass;
	CLOSE puntero_cus_ass;
	CLOSE puntero_ets_ass;

	
		
end $$

DELIMITER ;