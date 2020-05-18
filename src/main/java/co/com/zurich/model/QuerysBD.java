package co.com.zurich.model;

import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.DateFormat;
import java.util.Date;

import co.com.zurich.controller.Destinatario;
import co.com.zurich.controller.SendMail;

public class QuerysBD extends ConexionBD {
	static Destinatario correoDestinatario;
	public String correo;
	public static String correoUser;

	// hacer que el metodo retorne un bool
	@SuppressWarnings("unused")
	static public boolean dataCompleta(String completo, String nomArchivo) throws Throwable {
		String error;
		String[] separacionDatos;
		// Se crea un vector el cual se llena con la separación del String que traemos
		// de la Clase (ReadFile)
		separacionDatos = completo.split("\n");
		
		
		Date date = new Date();
		// Se instancian varios formatos de fecha el cual se utilizaran mas adelante
		DateFormat hourdateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		// instanciamos el preparedStatement ya que contienen una sentencia SQL que ya
		// ha sido compilada
		PreparedStatement preparedStatement = null;
		// instanciamos la clase Connection la cual representa la conexión con la Base
		// de Datos
		Connection conexion;

		ConexionBD miConexion = new ConexionBD();
		// asignamos la conexión a nuesta BD
		conexion = miConexion.getConnection();
		try {
			// A nuestra conexión le asignamos el AutoCommit en false, para poder utilizar
			// el commit como el rollback, depende de el proceso
			conexion.setAutoCommit(false);

			// se instancian variables que se usan mas adelante para unos inserts
			int idClient = 0;
			int consecutivo = 0;
			int idFuncionario = 0;
			int idRadicacion = 0;
			int clienteIdentCompleta = 0;
			int temp = 0;
			boolean documentoRepetido = false;
			int idClientregistrado = 0;
			// Se instancia el ResultSet, ya que trae el resultado de algunas consultas
			ResultSet rs;

			// Se crea un ciclo for el cual va rrecorrer todas las lineas una a una que
			// tiene nuestro .txt
			for (int i = 0; i < separacionDatos.length; i++) {
				String[] separacionDatosInsert;
//				separacionDatosInsert = separacionDatos[i].split("\\|");
				separacionDatosInsert = separacionDatos[i].split("\\|",999);

				if (temp == 0) {
					correoDestinatario = new Destinatario();
					correoDestinatario.setCorreo(separacionDatosInsert[10]);
					temp = 1;
				}

				try {

					if (separacionDatosInsert[0].equals("R1")) {
						clienteIdentCompleta = 0;
						idClient = 0;
						consecutivo = 0;
						idFuncionario = 0;
						idRadicacion = 0;
						documentoRepetido = false;

						/*
						 * ***********************
						 * 
						 * * = SELECCIONAR EL DOCUMNETO Y VALIDAR SI YA ESTA REGISTRADO EN LA DB = *
						 *
						 * ********************
						 */

						preparedStatement = conexion
								.prepareStatement("SELECT documento FROM asistemyca_zurich.clientes where documento = '"
										+ separacionDatosInsert[2] + "'");
						preparedStatement.executeQuery();
						rs = preparedStatement.getResultSet();
						if (rs.next()) {
							documentoRepetido = true;
						} else {
							documentoRepetido = false;
						}

						clienteIdentCompleta = Integer.parseInt(separacionDatosInsert[1]);
						/*
						 * ***********************
						 * 
						 * * = SELECCIONAR EL ID DEL CLIENTE A MODIFICAR = *
						 *
						 * ********************
						 */

						preparedStatement = conexion
								.prepareStatement("SELECT id FROM asistemyca_zurich.clientes where documento = '"
										+ separacionDatosInsert[2] + "'");
						preparedStatement.executeQuery();
						rs = preparedStatement.getResultSet();
						if (rs.next()) {
							idClientregistrado = rs.getInt(1);
						} else {
						}

						if (documentoRepetido == false) {

							/*
							 * ***********************
							 * 
							 * = INSERT R1 = * * = INSERT clientes = *
							 *
							 * ********************
							 */

							String sqlInsertClientes = "INSERT INTO `asistemyca_zurich`.`clientes` (`tipo_documento`, `documento`, `created`)"
									+ "VALUES (?, ?,'" + hourdateFormat.format(date) + "')";
							preparedStatement = conexion.prepareStatement(sqlInsertClientes,
									preparedStatement.RETURN_GENERATED_KEYS); // for insert
							preparedStatement.setInt(1, Integer.parseInt(separacionDatosInsert[1])); // tipo_documento
							preparedStatement.setString(2, separacionDatosInsert[2]); // documento

							preparedStatement.executeUpdate();

							rs = preparedStatement.getGeneratedKeys();
							if (rs.next()) {
								idClient = rs.getInt(1);
								System.out.println("ID Autogenerado:  " + idClient);
							}

							preparedStatement = conexion
									.prepareStatement("SELECT max(consecutivo)+1 FROM asistemyca_zurich.zr_radicacion");
							preparedStatement.executeQuery();
							rs = preparedStatement.getResultSet();
							if (rs.next()) {
								consecutivo = rs.getInt(1);
								System.out.println("consecutivo Autogenerado:  " + consecutivo);

							}

							preparedStatement = conexion
									.prepareStatement("SELECT id FROM asistemyca_zurich.users where correo = '"
											+ separacionDatosInsert[10] + "'");
							preparedStatement.executeQuery();
							rs = preparedStatement.getResultSet();
							if (rs.next()) {
								idFuncionario = rs.getInt(1);
								System.out.println("idFuncionario:  " + idFuncionario);
							}

							/*
							 * **********************************************
							 * 
							 * = INSERCCIÓN TABLA GESTION_CLIENTE_CAPTURA = *
							 *
							 * *******************************************
							 */

							String sqlAnexosGestionClientes = "INSERT INTO `asistemyca_zurich`.`gestion_clientes_captura` (`GESTION_USUARIO_ID`, `GESTION_CLIENTE_ID`, `GESTION_FECHA_DILIGENCIAMIENTO`, `FECHA_GESTION`)"
									+ " VALUES (?,?,'" + hourdateFormat.format(date) + "', '"
									+ hourdateFormat.format(date) + "')";
							preparedStatement = conexion.prepareStatement(sqlAnexosGestionClientes); // for insert
							preparedStatement.setInt(1, idFuncionario); // GESTION_USUARIO_ID
							preparedStatement.setInt(2, idClient); // GESTION_CLIENTE_ID
							preparedStatement.executeUpdate();

							/*
							 * *********************************************************
							 * 
							 * = INSERCCIÓN TABLA zr_estado_proceso_clientes_sarlaft = *
							 *
							 * *******************************************************
							 */

							String sqlAnexosZrEstadoProceso = "INSERT INTO `asistemyca_zurich`.`zr_estado_proceso_clientes_sarlaft`"
									+ "(`PROCESO_USUARIO_ID`, `PROCESO_CLIENTE_ID`, `PROCESO_FECHA_DILIGENCIAMIENTO`, `ESTADO_PROCESO_ID`, `PROCESO_ACTIVO`,"
									+ "`FECHA_PROCESO`)" + " VALUES (?,?,'" + hourdateFormat.format(date) + "', ?, ?, '"
									+ hourdateFormat.format(date) + "')";
							preparedStatement = conexion.prepareStatement(sqlAnexosZrEstadoProceso); // for insert

							preparedStatement.setInt(1, idFuncionario); // PROCESO_USUARIO_ID
							preparedStatement.setInt(2, idClient); // PROCESO_CLIENTE_ID
							preparedStatement.setInt(3, 1); // ESTADO_PROCESO_ID -- proceso captura
							preparedStatement.setInt(4, 1); // PROCESO_ACTIVO
							preparedStatement.executeUpdate();

							/*
							 * ***********************************
							 * 
							 * = INSERCION TABLA zr_radicacion = *
							 *
							 * ********************************
							 */

							String sqlZrRadicacion = "INSERT INTO `asistemyca_zurich`.`zr_radicacion` ("
									+ "`funcionario_id`, `cliente_id`, `consecutivo`,`numero_planilla`, `tipo_cliente`, "
									+ "`tipo_medio`, `devuelto`, `separado`, `digitalizado`, `cantidad_separada`, `formulario`,"
									+ " `cantidad_documentos`, `medio_recepcion` , `radicacion_proceso`,  `correo_radicacion`,"
									+ "`linea_negocio_id`, `radicacion_observacion`, `formulario_sarlaft` , `formulario_repetido`, `fecha_diligenciamiento`, `created`)"

									+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + "'"
									+ dateFormat.format(date) + "', '" + hourdateFormat.format(date) + "')";

							preparedStatement = conexion.prepareStatement(sqlZrRadicacion.toString(),
									preparedStatement.RETURN_GENERATED_KEYS); // for insert

							if (separacionDatosInsert[5].equalsIgnoreCase("")) {
								separacionDatosInsert[5] = "0";
							}
							if (separacionDatosInsert[11].equalsIgnoreCase("")) {
								separacionDatosInsert[11] = "1";
							}
							if (separacionDatosInsert[12].equalsIgnoreCase("")) {
								separacionDatosInsert[12] = "null";
							}
							preparedStatement.setInt(1, idFuncionario); // funcionario_id
							preparedStatement.setInt(2, idClient); // cliente_id
							preparedStatement.setInt(3, consecutivo); // consecutivo
							preparedStatement.setString(4, "prueba"); // numero_planilla
							preparedStatement.setString(5, "Cliente");// tipo_cliente
							preparedStatement.setString(6, separacionDatosInsert[3]); // tipo_medio
							preparedStatement.setString(7, "NO"); // devuelto
							preparedStatement.setString(8, separacionDatosInsert[4]); // separado
							preparedStatement.setString(9, "NO"); // digitalizado
							preparedStatement.setInt(10, Integer.parseInt(separacionDatosInsert[5])); // cantidad_separada
							preparedStatement.setString(11, separacionDatosInsert[6]); // formulario
							preparedStatement.setString(12, separacionDatosInsert[7]); // cantidad_documentos
							preparedStatement.setString(13, separacionDatosInsert[8]); // medio_recepcion
							if (separacionDatosInsert[9].equalsIgnoreCase("")) {
								preparedStatement.setNull(14, java.sql.Types.NULL ); // ppes_fecha_ingreso
							}else {
								preparedStatement.setString(14, separacionDatosInsert[9]); // radicacion_proceso
							}
							preparedStatement.setString(15, separacionDatosInsert[10]); // correo_radicacion
							preparedStatement.setInt(16, Integer.parseInt(separacionDatosInsert[11])); // linea_negocio_id
							preparedStatement.setString(17, separacionDatosInsert[12]); // radicacion_observacion
							preparedStatement.setInt(18, 1); // formulario_sarlaft
							preparedStatement.setInt(19, 0); // formulario_repetido

							preparedStatement.executeUpdate();

							correoUser = separacionDatosInsert[9];
							rs = preparedStatement.getGeneratedKeys();
							if (rs.next()) {
								idRadicacion = rs.getInt(1);
								System.out.println("ID idRadicacion:  " + idRadicacion);
							}

							if (Integer.parseInt(separacionDatosInsert[1]) == 3
									|| Integer.parseInt(separacionDatosInsert[1]) == 9) {

								/*
								 * *************************************
								 * 
								 * = INSERT cliente_sarlaft_juridico = *
								 *
								 * ***********************************
								 */

								String sqlClienteJuridico = "INSERT INTO `asistemyca_zurich`.`cliente_sarlaft_juridico` ("
										+ "`cliente`, `ciudad_diligenciamiento`, `sucursal`, `tipo_solicitud`, `residencia_sociedad`, `clase_vinculacion`,"
										+ " `clase_vinculacion_otro`, `relacion_tom_asegurado`, `relacion_tom_asegurado_otra`, `relacion_tom_beneficiario`, "
										+ "`relacion_tom_beneficiario_otra`,`relacion_aseg_beneficiario`, `relacion_aseg_beneficiario_otra`, `razon_social`, `info_basica_tipo_sociedad`,"
										+ " `ofi_principal_direccion`, `ofi_principal_tipo_empresa`, `ofi_principal_departamento_empresa`, `ofi_principal_ciudad_empresa`, "
										+ "`ofi_principal_telefono`, `ofi_principal_fax`, `ofi_principal_pagina_web`, `ofi_principal_email`, `ofi_principal_ciiu`, "
										+ "`ofi_principal_ciiu_cod`, `ofi_principal_sector`, `sucursal_direccion`, `sucursal_departamento`, `sucursal_ciudad`,"
										+ " `sucursal_telefono`,  `rep_legal_primer_apellido`, `rep_legal_segundo_apellido`, `rep_legal_nombres`, `rep_legal_tipo_documento`,"
										+ " `rep_legal_documento`, `rep_legal_fecha_exp_documento`, `rep_legal_lugar_expedicion`, `rep_legal_fecha_nacimiento`, "
										+ "`rep_legal_lugar_nacimiento`, `rep_legal_nacionalidad_1`, `rep_legal_email`, `rep_legal_direccion_residencia`, "
										+ "`rep_legal_pais_residencia`, `rep_legal_departamento_residencia`, `rep_legal_ciudad_residencia`, `rep_legal_telefono_residencia`,"
										+ " `rep_legal_celular_residencia`, `rep_legal_persona_publica`, `rep_legal_recursos_publicos`, `rep_legal_obligaciones_tributarias`,"
										+ " `rep_legal_obligaciones_tributarias_indique`, `anexo_accionistas`, `anexo_sub_accionistas`, `ingresos`,"
										+ " `egresos`, `activos`, `pasivos`, `patrimonio`, `otros_ingresos`, `desc_otros_ingresos`,`tipo_moneda`, `anexo_preguntas_ppes`, "
										+ "`operaciones_moneda_extranjera`, `cuentas_moneda_exterior`, `productos_exterior`, `reclamaciones`, "
										+ "`reclamacion_anio`, `reclamacion_ramo`, `reclamacion_compania`, `reclamacion_valor`,`reclamacion_resultado`, `reclamacion_anio_2`,"
										+ " `reclamacion_ramo_2`, `reclamacion_compania_2`, `reclamacion_valor_2`, `reclamacion_resultado_2`, `chk_formulario_sarlaft`, "
										+ "`chk_documentos`, `otro`, `huella`, `firma`, `entrevista`, `verificacion`, `autoriza_info_fasecolda`, `autoriza_tratamiento`) "
										+ "VALUES (" + "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
										+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
										+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
										+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," + "?,?,?,?,?,?) ";

								preparedStatement = conexion.prepareStatement(sqlClienteJuridico); // for insert

								if (separacionDatosInsert[13].equalsIgnoreCase("")) {
									separacionDatosInsert[13] = "0";
								}
								if (separacionDatosInsert[26].equalsIgnoreCase("")) {
									separacionDatosInsert[26] = "0";
								}
								if (separacionDatosInsert[29].equalsIgnoreCase("")) {
									separacionDatosInsert[29] = "0";
								}
								if (separacionDatosInsert[30].equalsIgnoreCase("")) {
									separacionDatosInsert[30] = "0";
								}
								if (separacionDatosInsert[31].equalsIgnoreCase("")) {
									separacionDatosInsert[31] = "0";
								}
								if (separacionDatosInsert[32].equalsIgnoreCase("")) {
									separacionDatosInsert[32] = "0";
								}
								if (separacionDatosInsert[37].equalsIgnoreCase("")) {
									separacionDatosInsert[37] = "0";
								}
								if (separacionDatosInsert[39].equalsIgnoreCase("")) {
									separacionDatosInsert[39] = "0";
								}
								if (separacionDatosInsert[40].equalsIgnoreCase("")) {
									separacionDatosInsert[40] = "0";
								}
								if (separacionDatosInsert[41].equalsIgnoreCase("")) {
									separacionDatosInsert[41] = "0";
								}
								if (separacionDatosInsert[45].equalsIgnoreCase("")) {
									separacionDatosInsert[45] = "0";
								}
								if (separacionDatosInsert[57].equalsIgnoreCase("")) {
									separacionDatosInsert[57] = "0";
								}
								if (separacionDatosInsert[58].equalsIgnoreCase("")) {
									separacionDatosInsert[58] = "0";
								}
								if (separacionDatosInsert[64].equalsIgnoreCase("")) {
									separacionDatosInsert[64] = "0";
								}
								if (separacionDatosInsert[93].equalsIgnoreCase("")) {
									separacionDatosInsert[93] = "0";
								}
								if (separacionDatosInsert[94].equalsIgnoreCase("")) {
									separacionDatosInsert[94] = "0";
								}
								if (separacionDatosInsert[95].equalsIgnoreCase("")) {
									separacionDatosInsert[95] = "0";
								}
								if (separacionDatosInsert[96].equalsIgnoreCase("")) {
									separacionDatosInsert[96] = "0";
								}
								if (separacionDatosInsert[97].equalsIgnoreCase("")) {
									separacionDatosInsert[97] = "0";
								}
								if (separacionDatosInsert[98].equalsIgnoreCase("")) {
									separacionDatosInsert[98] = "0";
								}
								if (separacionDatosInsert[106].equalsIgnoreCase("")) {
									separacionDatosInsert[106] = "0";
								}
								if (separacionDatosInsert[110].equalsIgnoreCase("")) {
									separacionDatosInsert[110] = "0";
								}
								if (separacionDatosInsert[111].equalsIgnoreCase("")) {
									separacionDatosInsert[111] = "0";
								}
								if (separacionDatosInsert[114].equalsIgnoreCase("")) {
									separacionDatosInsert[114] = "0";
								}
								if (separacionDatosInsert[115].equalsIgnoreCase("")) {
									separacionDatosInsert[115] = "0";
								}
								if (separacionDatosInsert[116].equalsIgnoreCase("")) {
									separacionDatosInsert[116] = "0";
								}
								if (separacionDatosInsert[119].equalsIgnoreCase("")) {
									separacionDatosInsert[119] = "0";
								}
								if (separacionDatosInsert[120].equalsIgnoreCase("")) {
									separacionDatosInsert[120] = "0";
								}
								if (separacionDatosInsert[121].equalsIgnoreCase("")) {
									separacionDatosInsert[121] = "0";
								}
								if (separacionDatosInsert[122].equalsIgnoreCase("")) {
									separacionDatosInsert[122] = "0";
								}
								if (separacionDatosInsert[123].equalsIgnoreCase("")) {
									separacionDatosInsert[123] = "0";
								}
								if (separacionDatosInsert[127].equalsIgnoreCase("")) {
									separacionDatosInsert[127] = "0";
								}
								if (separacionDatosInsert[128].equalsIgnoreCase("")) {
									separacionDatosInsert[128] = "0";
								}   
								preparedStatement.setInt(1, idClient); // cliente
								preparedStatement.setInt(2, Integer.parseInt(separacionDatosInsert[13])); // ciudad_diligenciamiento
								preparedStatement.setString(3, separacionDatosInsert[14]); // sucursal
								preparedStatement.setString(4, separacionDatosInsert[15]); // tipo_solicitud
								preparedStatement.setString(5, separacionDatosInsert[16]); // residencia_sociedad
								preparedStatement.setString(6, separacionDatosInsert[17]); // clase_vinculacion
								preparedStatement.setString(7, separacionDatosInsert[18]); // clase_vinculacion_otro
								preparedStatement.setString(8, separacionDatosInsert[19]); // relacion_tom_asegurado
								preparedStatement.setString(9, separacionDatosInsert[20]); // relacion_tom_asegurado_otra
								preparedStatement.setString(10, separacionDatosInsert[21]); // relacion_tom_beneficiario
								preparedStatement.setString(11, separacionDatosInsert[22]); // relacion_tom_beneficiario_otra
								preparedStatement.setString(12, separacionDatosInsert[23]); // relacion_aseg_beneficiario
								preparedStatement.setString(13, separacionDatosInsert[24]); // relacion_aseg_beneficiario_otra
								preparedStatement.setString(14, separacionDatosInsert[25]); // razon_social
								preparedStatement.setInt(15, Integer.parseInt(separacionDatosInsert[26])); // info_basica_tipo_sociedad
								preparedStatement.setString(16, separacionDatosInsert[27]); // ofi_principal_direccion
								preparedStatement.setString(17, separacionDatosInsert[28]); // ofi_principal_tipo_empresa
								preparedStatement.setInt(18, Integer.parseInt(separacionDatosInsert[29])); // ofi_principal_departamento_empresa
								preparedStatement.setInt(19, Integer.parseInt(separacionDatosInsert[30])); // ofi_principal_ciudad_empresa
								preparedStatement.setInt(20, Integer.parseInt(separacionDatosInsert[31])); // ofi_principal_telefono
								preparedStatement.setInt(21, Integer.parseInt(separacionDatosInsert[32])); // ofi_principal_fax
								preparedStatement.setString(22, separacionDatosInsert[33]); // ofi_principal_pagina_web
								preparedStatement.setString(23, separacionDatosInsert[34]); // ofi_principal_email
								preparedStatement.setString(24, separacionDatosInsert[35]); // ofi_principal_ciiu
								preparedStatement.setString(25, separacionDatosInsert[36]); // ofi_principal_ciiu_cod
								preparedStatement.setInt(26, Integer.parseInt(separacionDatosInsert[37])); // ofi_principal_sector
								preparedStatement.setString(27, separacionDatosInsert[38]); // sucursal_direccion
								preparedStatement.setInt(28, Integer.parseInt(separacionDatosInsert[39])); // sucursal_departamento
								preparedStatement.setInt(29, Integer.parseInt(separacionDatosInsert[40])); // sucursal_ciudad
								preparedStatement.setInt(30, Integer.parseInt(separacionDatosInsert[41])); // sucursal_telefono
								preparedStatement.setString(31, separacionDatosInsert[42]); // rep_legal_primer_apellido
								preparedStatement.setString(32, separacionDatosInsert[43]); // rep_legal_segundo_apellido
								preparedStatement.setString(33, separacionDatosInsert[44]); // rep_legal_nombres
								preparedStatement.setInt(34, Integer.parseInt(separacionDatosInsert[45])); // rep_legal_tipo_documento
								preparedStatement.setString(35, separacionDatosInsert[46]); // rep_legal_documento
								if (separacionDatosInsert[47].equalsIgnoreCase("")) {
									preparedStatement.setNull(36, java.sql.Types.DATE ); // ppes_fecha_ingreso
								}else {
									preparedStatement.setString(36, separacionDatosInsert[47]); // rep_legal_fecha_exp_documento
								}
//								preparedStatement.setString(36, separacionDatosInsert[47]); // rep_legal_fecha_exp_documento
								preparedStatement.setString(37, separacionDatosInsert[48]); // rep_legal_lugar_expedicion
								if (separacionDatosInsert[49].equalsIgnoreCase("")) {
									preparedStatement.setNull(38, java.sql.Types.DATE ); // ppes_fecha_ingreso
								}else {
									preparedStatement.setString(38, separacionDatosInsert[49]); // rep_legal_fecha_nacimiento
								}
//								preparedStatement.setString(38, separacionDatosInsert[49]); // rep_legal_fecha_nacimiento
								preparedStatement.setString(39, separacionDatosInsert[50]); // rep_legal_lugar_nacimiento
								preparedStatement.setInt(40, Integer.parseInt(separacionDatosInsert[51])); // rep_legal_nacionalidad_1
								preparedStatement.setString(41, separacionDatosInsert[52]); // rep_legal_email
								preparedStatement.setString(42, separacionDatosInsert[53]); // rep_legal_direccion_residencia
								preparedStatement.setInt(43, Integer.parseInt(separacionDatosInsert[54])); // rep_legal_pais_residencia
								preparedStatement.setInt(44, Integer.parseInt(separacionDatosInsert[55])); // rep_legal_departamento_residencia
								preparedStatement.setInt(45, Integer.parseInt(separacionDatosInsert[56])); // rep_legal_ciudad_residencia
								preparedStatement.setInt(46, Integer.parseInt(separacionDatosInsert[57])); // rep_legal_telefono_residencia
								preparedStatement.setInt(47, Integer.parseInt(separacionDatosInsert[58])); // rep_legal_celular_residencia
								preparedStatement.setString(48, separacionDatosInsert[59]); // rep_legal_persona_publica
								preparedStatement.setString(49, separacionDatosInsert[60]); // rep_legal_recursos_publicos
								preparedStatement.setString(50, separacionDatosInsert[61]); // rep_legal_obligaciones_tributarias
								preparedStatement.setString(51, separacionDatosInsert[62]); // rep_legal_obligaciones_tributarias_indique
								preparedStatement.setString(52, separacionDatosInsert[63]); // anexo_accionistas
								preparedStatement.setInt(53, Integer.parseInt(separacionDatosInsert[64])); // anexo_sub_accionistas
								preparedStatement.setInt(54, Integer.parseInt(separacionDatosInsert[93])); // ingresos
								preparedStatement.setInt(55, Integer.parseInt(separacionDatosInsert[94])); // egresos
								preparedStatement.setInt(56, Integer.parseInt(separacionDatosInsert[95])); // activos
								preparedStatement.setInt(57, Integer.parseInt(separacionDatosInsert[96])); // pasivos
								preparedStatement.setInt(58, Integer.parseInt(separacionDatosInsert[97])); // patrimonio
								preparedStatement.setInt(59, Integer.parseInt(separacionDatosInsert[98])); // otros_ingresos
								preparedStatement.setString(60, separacionDatosInsert[99]); // desc_otros_ingresos
								preparedStatement.setString(61, separacionDatosInsert[100]); // tipo_moneda
								preparedStatement.setInt(62, Integer.parseInt(separacionDatosInsert[106])); // anexo_preguntas_ppes
								preparedStatement.setString(63, separacionDatosInsert[107]); // operaciones_moneda_extranjera
								preparedStatement.setString(64, separacionDatosInsert[108]); // cuentas_moneda_exterior
								preparedStatement.setString(65, separacionDatosInsert[109]); // productos_exterior
								preparedStatement.setInt(66, Integer.parseInt(separacionDatosInsert[110])); // reclamaciones
								preparedStatement.setInt(67, Integer.parseInt(separacionDatosInsert[111])); // reclamacion_anio
								preparedStatement.setString(68, separacionDatosInsert[112]); // reclamacion_ramo
								preparedStatement.setString(69, separacionDatosInsert[113]); // reclamacion_compania
								preparedStatement.setInt(70, Integer.parseInt(separacionDatosInsert[114])); // reclamacion_valor
								preparedStatement.setInt(71, Integer.parseInt(separacionDatosInsert[115])); // reclamacion_resultado
								preparedStatement.setString(72, separacionDatosInsert[116]); // reclamacion_anio_2
								preparedStatement.setString(73, separacionDatosInsert[117]); // reclamacion_ramo_2
								preparedStatement.setString(74, separacionDatosInsert[118]); // reclamacion_compania_2
								preparedStatement.setInt(75, Integer.parseInt(separacionDatosInsert[119])); // reclamacion_valor_2
								preparedStatement.setInt(76, Integer.parseInt(separacionDatosInsert[120])); // reclamacion_resultado_2
								preparedStatement.setInt(77, Integer.parseInt(separacionDatosInsert[121])); // chk_formulario_sarlaft
								preparedStatement.setInt(78, Integer.parseInt(separacionDatosInsert[122])); // chk_documentos
								preparedStatement.setInt(79, Integer.parseInt(separacionDatosInsert[123])); // otro
								preparedStatement.setInt(80, Integer.parseInt(separacionDatosInsert[125])); // huella
								preparedStatement.setInt(81, Integer.parseInt(separacionDatosInsert[126])); // firma
								preparedStatement.setInt(82, Integer.parseInt(separacionDatosInsert[127])); // entrevista
								preparedStatement.setInt(83, Integer.parseInt(separacionDatosInsert[128])); // verificacion
								preparedStatement.setString(84, separacionDatosInsert[129]); // autoriza_info_fasecolda
								preparedStatement.setString(85, separacionDatosInsert[130]); // autoriza_tratamiento
								preparedStatement.executeUpdate();
							} else {

								/*
								 * ************************************
								 * 
								 * = INSERT cliente_sarlaft_natural = *
								 *
								 * *********************************
								 */

								String sqlClienteNatural = "INSERT INTO `asistemyca_zurich`.`cliente_sarlaft_natural` ("
										+ "`cliente`, `ciudad_diligenciamiento`, `sucursal`, `tipo_solicitud`, `clase_vinculacion`, "
										+ "`clase_vinculacion_otro`, `relacion_tom_asegurado`,"
										+ "`relacion_tom_asegurado_otra`, `relacion_tom_beneficiario`,`relacion_tom_beneficiario_otra`, `relacion_aseg_beneficiario`,"
										+ " `relacion_aseg_beneficiario_otra`, `primer_apellido`, `segundo_apellido`, `primer_nombre`,"
										+ " `segundo_nombre`, `sexo`, `estado_civil`, `fecha_expedicion_documento`, `lugar_expedicion_documento`, `fecha_nacimiento`, "
										+ "`lugar_nacimiento`, `nacionalidad_1`, `ocupacion`, `direccion_residencia`, `departamento_residencia`, "
										+ "`ciudad_residencia`, `telefono`, `celular`, `correo_electronico`, `actividad_eco_principal`, `trabaja_actualmente`,"
										+ " `sector`, `tipo_actividad`, `cargo`, `empresa_donde_trabaja`, `departamento_empresa`,"
										+ " `ciudad_empresa`, `direccion_empresa`, `telefono_empresa`, `ingresos`, `egresos`, `activos`, `pasivos`, `patrimonio`, "
										+ "`otros_ingresos`, `desc_otros_ingresos`, `tipo_moneda`, `persona_publica`, `vinculo_persona_publica`, `productos_publicos`,"
										+ " `obligaciones_tributarias_otro_pais`, `desc_obligaciones_tributarias_otro_pais`, `anexo_preguntas_ppes`, "
										+ "`operaciones_moneda_extranjera`, `cuentas_moneda_exterior`, `productos_exterior`, `reclamaciones`, `reclamacion_anio`, "
										+ "`reclamacion_ramo`, `reclamacion_compania`, `reclamacion_valor`, `reclamacion_resultado`, `reclamacion_anio_2`, `reclamacion_ramo_2`,"
										+ " `reclamacion_compania_2`, `reclamacion_valor_2`, `reclamacion_resultado_2`, `chk_formulario_sarlaft`, `chk_documentos`, `otro`,"
										+ " `tipo_documento_otro`, `huella`, `firma`, `entrevista`, `verificacion`, `autoriza_info_fasecolda`, `autoriza_tratamiento`)"
										+ "VALUES (" + "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
										+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
										+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
										+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

								preparedStatement = conexion.prepareStatement(sqlClienteNatural); // for insert
								// set values
								if (separacionDatosInsert[13].equalsIgnoreCase("")) {
									separacionDatosInsert[13] = "0";
								}
								if (separacionDatosInsert[75].equalsIgnoreCase("")) {
									separacionDatosInsert[75] = "0";
								}
								if (separacionDatosInsert[78].equalsIgnoreCase("")) {
									separacionDatosInsert[78] = "0";
								}
								if (separacionDatosInsert[79].equalsIgnoreCase("")) {
									separacionDatosInsert[79] = "0";
								}
								if (separacionDatosInsert[80].equalsIgnoreCase("")) {
									separacionDatosInsert[80] = "000000";
								}
								if (separacionDatosInsert[81].equalsIgnoreCase("")) {
									separacionDatosInsert[81] = "0";
								}
								if (separacionDatosInsert[85].equalsIgnoreCase("")) {
									separacionDatosInsert[85] = "0";
								}
								if (separacionDatosInsert[86].equalsIgnoreCase("")) {
									separacionDatosInsert[86] = "0";
								}
								if (separacionDatosInsert[89].equalsIgnoreCase("")) {
									separacionDatosInsert[89] = "0";
								}
								if (separacionDatosInsert[90].equalsIgnoreCase("")) {
									separacionDatosInsert[90] = "0";
								}
								if (separacionDatosInsert[92].equalsIgnoreCase("")) {
									separacionDatosInsert[92] = "000000";
								}
								if (separacionDatosInsert[93].equalsIgnoreCase("")) {
									separacionDatosInsert[93] = "0";
								}
								if (separacionDatosInsert[94].equalsIgnoreCase("")) {
									separacionDatosInsert[94] = "0";
								}
								if (separacionDatosInsert[95].equalsIgnoreCase("")) {
									separacionDatosInsert[95] = "0";
								}
								if (separacionDatosInsert[96].equalsIgnoreCase("")) {
									separacionDatosInsert[96] = "0";
								}
								if (separacionDatosInsert[97].equalsIgnoreCase("")) {
									separacionDatosInsert[97] = "0";
								}
								if (separacionDatosInsert[98].equalsIgnoreCase("")) {
									separacionDatosInsert[98] = "0";
								}
								if (separacionDatosInsert[106].equalsIgnoreCase("")) {
									separacionDatosInsert[106] = "0";
								}
								if (separacionDatosInsert[110].equalsIgnoreCase("")) {
									separacionDatosInsert[110] = "0";
								}
								if (separacionDatosInsert[111].equalsIgnoreCase("")) {
									separacionDatosInsert[111] = "0";
								}
								if (separacionDatosInsert[114].equalsIgnoreCase("")) {
									separacionDatosInsert[114] = "0";
								}
								if (separacionDatosInsert[115].equalsIgnoreCase("")) {
									separacionDatosInsert[115] = "0";
								}
								if (separacionDatosInsert[116].equalsIgnoreCase("")) {
									separacionDatosInsert[116] = "0";
								}
								if (separacionDatosInsert[119].equalsIgnoreCase("")) {
									separacionDatosInsert[119] = "0";
								}
								if (separacionDatosInsert[120].equalsIgnoreCase("")) {
									separacionDatosInsert[120] = "0";
								}
								if (separacionDatosInsert[121].equalsIgnoreCase("")) {
									separacionDatosInsert[121] = "0";
								}
								if (separacionDatosInsert[122].equalsIgnoreCase("")) {
									separacionDatosInsert[122] = "0";
								}
								if (separacionDatosInsert[123].equalsIgnoreCase("")) {
									separacionDatosInsert[123] = "0";
								}
								if (separacionDatosInsert[127].equalsIgnoreCase("")) {
									separacionDatosInsert[127] = "0";
								}
								if (separacionDatosInsert[128].equalsIgnoreCase("")) {
									separacionDatosInsert[128] = "0";
								}  
								preparedStatement.setInt(1, idClient); // cliente|
								preparedStatement.setInt(2, Integer.parseInt(separacionDatosInsert[13])); // ciudad_diligenciamiento|
								preparedStatement.setString(3, separacionDatosInsert[14]); // sucursal
								preparedStatement.setString(4, separacionDatosInsert[15]);// tipo_solicitud
								preparedStatement.setString(5, separacionDatosInsert[17]); // clase_vinculacion
								preparedStatement.setString(6, separacionDatosInsert[18]); // clase_vinculacion_otro
								preparedStatement.setString(7, separacionDatosInsert[19]); // relacion_tom_asegurado
								preparedStatement.setString(8, separacionDatosInsert[20]); // relacion_tom_asegurado_otra
								preparedStatement.setString(9, separacionDatosInsert[21]); // relacion_tom_beneficiario
								preparedStatement.setString(10, separacionDatosInsert[22]); // relacion_tom_beneficiario_otra
								preparedStatement.setString(11, separacionDatosInsert[23]); // relacion_aseg_beneficiario
								preparedStatement.setString(12, separacionDatosInsert[24]); // relacion_aseg_beneficiario_otra
								preparedStatement.setString(13, separacionDatosInsert[65]); // primer_apellido
								preparedStatement.setString(14, separacionDatosInsert[66].toString()); // segundo_apellido
								preparedStatement.setString(15, separacionDatosInsert[67].toString()); // primer_nombre
								preparedStatement.setString(16, separacionDatosInsert[68].toString()); // segundo_nombre
								preparedStatement.setString(17, separacionDatosInsert[69]); // sexo
								if (separacionDatosInsert[70].equalsIgnoreCase("")) {
									preparedStatement.setInt(18, java.sql.Types.NULL); // estado_civil
								}else {
									preparedStatement.setInt(18, Integer.parseInt(separacionDatosInsert[70])); // estado_civil
								}								
								if (separacionDatosInsert[71].equalsIgnoreCase("")) {
									preparedStatement.setNull(19, java.sql.Types.DATE ); // ppes_fecha_ingreso
								}else {
									preparedStatement.setString(19, separacionDatosInsert[71]); // fecha_expedicion_documento
								}
//								preparedStatement.setString(19, separacionDatosInsert[71]); // fecha_expedicion_documento
								preparedStatement.setString(20, separacionDatosInsert[72]); // lugar_expedicion_documento
								if (separacionDatosInsert[73].equalsIgnoreCase("")) {
									preparedStatement.setNull(21, java.sql.Types.DATE ); // ppes_fecha_ingreso
								}else {
									preparedStatement.setString(21, separacionDatosInsert[73]); // fecha_nacimiento
								}
//								preparedStatement.setString(21, separacionDatosInsert[73]); // fecha_nacimiento
								preparedStatement.setString(22, separacionDatosInsert[74]); // lugar_nacimiento
								preparedStatement.setInt(23, Integer.parseInt(separacionDatosInsert[75])); // nacionalidad_1
								preparedStatement.setString(24, separacionDatosInsert[76]); // ocupacion
								preparedStatement.setString(25, separacionDatosInsert[77]); // direccion_residencia
								preparedStatement.setInt(26, Integer.parseInt(separacionDatosInsert[78])); // departamento_residencia
								preparedStatement.setInt(27, Integer.parseInt(separacionDatosInsert[79])); // ciudad_residencia
								preparedStatement.setInt(28, Integer.parseInt(separacionDatosInsert[80])); // telefono
								preparedStatement.setLong(29, Long.parseLong(separacionDatosInsert[81])); // celular
								preparedStatement.setString(30, separacionDatosInsert[82]); // correo_electronico
								preparedStatement.setString(31, separacionDatosInsert[83]); // actividad_eco_principal
								preparedStatement.setInt(32, Integer.parseInt(separacionDatosInsert[84])); // trabaja_actualmente
								preparedStatement.setInt(33, Integer.parseInt(separacionDatosInsert[85])); // sector
								preparedStatement.setInt(34, Integer.parseInt(separacionDatosInsert[86])); // tipo_actividad
								preparedStatement.setString(35, separacionDatosInsert[87]); // cargo
								preparedStatement.setString(36, separacionDatosInsert[88]); // empresa_donde_trabaja
								preparedStatement.setInt(37, Integer.parseInt(separacionDatosInsert[89])); // departamento_empresa
								preparedStatement.setInt(38, Integer.parseInt(separacionDatosInsert[90])); // ciudad_empresa
								preparedStatement.setString(39, separacionDatosInsert[91]); // direccion_empresa
								preparedStatement.setInt(40, Integer.parseInt(separacionDatosInsert[92])); // telefono_empresa
								preparedStatement.setInt(41, Integer.parseInt(separacionDatosInsert[93])); // ingresos
								preparedStatement.setInt(42, Integer.parseInt(separacionDatosInsert[94])); // egresos
								preparedStatement.setInt(43, Integer.parseInt(separacionDatosInsert[95])); // activos
								preparedStatement.setInt(44, Integer.parseInt(separacionDatosInsert[96])); // pasivos
								preparedStatement.setInt(45, Integer.parseInt(separacionDatosInsert[97])); // patrimonio
								preparedStatement.setInt(46, Integer.parseInt(separacionDatosInsert[98])); // otros_ingresos
								preparedStatement.setString(47, separacionDatosInsert[99]); // desc_otros_ingresos
								preparedStatement.setString(48, separacionDatosInsert[100]); // tipo_moneda
								preparedStatement.setString(49, separacionDatosInsert[101]); // persona_publica
								preparedStatement.setString(50, separacionDatosInsert[102]); // vinculo_persona_publica
								preparedStatement.setString(51, separacionDatosInsert[103]); // productos_publicos
								preparedStatement.setString(52, separacionDatosInsert[104]); // obligaciones_tributarias_otro_pais
								preparedStatement.setString(53, separacionDatosInsert[105]); // desc_obligaciones_tributarias_otro_pais
								preparedStatement.setInt(54, Integer.parseInt(separacionDatosInsert[106])); // anexo_preguntas_ppes
								preparedStatement.setString(55, separacionDatosInsert[107]); // operaciones_moneda_extranjera
								preparedStatement.setString(56, separacionDatosInsert[108]); // cuentas_moneda_exterior
								preparedStatement.setString(57, separacionDatosInsert[109]); // productos_exterior
								preparedStatement.setInt(58, Integer.parseInt(separacionDatosInsert[110])); // reclamaciones
								preparedStatement.setInt(59, Integer.parseInt(separacionDatosInsert[111])); // reclamacion_anio
								preparedStatement.setString(60, separacionDatosInsert[112]); // reclamacion_ramo
								preparedStatement.setString(61, separacionDatosInsert[113]); // reclamacion_compania
								preparedStatement.setString(62, separacionDatosInsert[114]); // reclamacion_valor x
								preparedStatement.setInt(63, Integer.parseInt(separacionDatosInsert[115])); // reclamacion_resultado
								preparedStatement.setInt(64, Integer.parseInt(separacionDatosInsert[116])); // reclamacion_anio_2
								preparedStatement.setString(65, separacionDatosInsert[117]); // reclamacion_ramo_2
								preparedStatement.setString(66, separacionDatosInsert[118]); // reclamacion_compania_2
								preparedStatement.setInt(67, Integer.parseInt(separacionDatosInsert[119])); // reclamacion_valor_2
								preparedStatement.setInt(68, Integer.parseInt(separacionDatosInsert[120])); // reclamacion_resultado_2
								preparedStatement.setInt(69, Integer.parseInt(separacionDatosInsert[121])); // chk_formulario_sarlaft
								preparedStatement.setInt(70, Integer.parseInt(separacionDatosInsert[122])); // chk_documentos
								preparedStatement.setInt(71, Integer.parseInt(separacionDatosInsert[123])); // otro
								preparedStatement.setString(72, separacionDatosInsert[124]); // tipo_documento_otro
								preparedStatement.setInt(73, Integer.parseInt(separacionDatosInsert[125])); // huella
								preparedStatement.setInt(74, Integer.parseInt(separacionDatosInsert[126])); // firma
								preparedStatement.setInt(75, Integer.parseInt(separacionDatosInsert[127])); // entrevista
								preparedStatement.setInt(76, Integer.parseInt(separacionDatosInsert[128])); // verificacion
								preparedStatement.setString(77, separacionDatosInsert[129]); // autoriza_info_fasecolda
								preparedStatement.setString(78, separacionDatosInsert[130]); // autoriza_tratamiento
								preparedStatement.executeUpdate();
							}
						} else {

							/*
							 * ********************
							 * 
							 * = UPDATE R1 =      *
							 * = UPDATE CLIENTE = *
							 *
							 * ****************** */

							String sqlUpdateClientes = "UPDATE `clientes` SET `tipo_documento` = ? WHERE `clientes`.`documento` = ?";
							preparedStatement = conexion.prepareStatement(sqlUpdateClientes); // for insert

							preparedStatement.setInt(1, Integer.parseInt(separacionDatosInsert[1])); // tipo_documento
							preparedStatement.setString(2, separacionDatosInsert[2]); // documento
							preparedStatement.executeUpdate();
//							rs = preparedStatement.getGeneratedKeys();
							System.out.println(separacionDatosInsert[1]);

							/*
							 * ********************************
							 * 
							 * = UPDATE TABLA zr_radicacion = *
							 *
							 * ********************************
							 */

							String sqlUpdateZrRadicacion = "UPDATE `zr_radicacion` SET "
									+ "`tipo_medio` =?, `separado` =?, `cantidad_separada` =?, `formulario` =?,"
									+ " `cantidad_documentos` =?, `medio_recepcion` =?, `radicacion_proceso` =?,  `correo_radicacion` =?,"
									+ "`linea_negocio_id` =?, `radicacion_observacion` =? WHERE `zr_radicacion`.`cliente_id` = ?";

							preparedStatement = conexion.prepareStatement(sqlUpdateZrRadicacion.toString()); // for
																												// insert

							if (separacionDatosInsert[5].equalsIgnoreCase("")) {
								separacionDatosInsert[5] = "0";
							}
							if (separacionDatosInsert[11].equalsIgnoreCase("")) {
								separacionDatosInsert[11] = "1";
							}
							if (separacionDatosInsert[12].equalsIgnoreCase("")) {
								separacionDatosInsert[12] = "null";
							}
							preparedStatement.setString(1, separacionDatosInsert[3]); // tipo_medio
							preparedStatement.setString(2, separacionDatosInsert[4]); // separado
							preparedStatement.setInt(3, Integer.parseInt(separacionDatosInsert[5])); // cantidad_separada
							preparedStatement.setString(4, separacionDatosInsert[6]); // formulario
							preparedStatement.setString(5, separacionDatosInsert[7]); // cantidad_documentos
							preparedStatement.setString(6, separacionDatosInsert[8]); // medio_recepcion
							if (separacionDatosInsert[9].equalsIgnoreCase("")) {
								preparedStatement.setNull(7, java.sql.Types.NULL ); // ppes_fecha_ingreso
							}else {
								preparedStatement.setString(7, separacionDatosInsert[9]); // radicacion_proceso
							}
//							preparedStatement.setString(7, separacionDatosInsert[9]); // radicacion_proceso
							preparedStatement.setString(8, separacionDatosInsert[10]); // correo_radicacion
							preparedStatement.setInt(9, Integer.parseInt(separacionDatosInsert[11])); // linea_negocio_id
							preparedStatement.setString(10, separacionDatosInsert[12]); // radicacion_observacion
							preparedStatement.setInt(11, idClientregistrado); // idClientregistrado
							preparedStatement.execute();
							
							
							if (Integer.parseInt(separacionDatosInsert[1]) == 3
									|| Integer.parseInt(separacionDatosInsert[1]) == 9) {

								/*
								 * *************************************
								 * 
								 * = UPDATE cliente_sarlaft_juridico = *
								 *
								 * *************************************
								 */

								String sqlClienteJuridico = "UPDATE `cliente_sarlaft_juridico` SET"
										+ "`ciudad_diligenciamiento` =?, `sucursal` =?, `tipo_solicitud` =?, `residencia_sociedad` =?, `clase_vinculacion` =?,"
										+ " `clase_vinculacion_otro` =?, `relacion_tom_asegurado` =?, `relacion_tom_asegurado_otra` =?, `relacion_tom_beneficiario` =?, "
										+ "`relacion_tom_beneficiario_otra`=?,`relacion_aseg_beneficiario`=?, `relacion_aseg_beneficiario_otra` =?, `razon_social` =?, `info_basica_tipo_sociedad` =?,"
										+ " `ofi_principal_direccion`=?, `ofi_principal_tipo_empresa`=?, `ofi_principal_departamento_empresa`=?, `ofi_principal_ciudad_empresa`=?, "
										+ "`ofi_principal_telefono`=?, `ofi_principal_fax`=?, `ofi_principal_pagina_web`=?, `ofi_principal_email`=?, `ofi_principal_ciiu`=?, "
										+ "`ofi_principal_ciiu_cod`=?, `ofi_principal_sector`=?, `sucursal_direccion`=?, `sucursal_departamento`=?, `sucursal_ciudad`=?,"
										+ " `sucursal_telefono`=?,  `rep_legal_primer_apellido`=?, `rep_legal_segundo_apellido`=?, `rep_legal_nombres`=?, `rep_legal_tipo_documento`=?,"
										+ " `rep_legal_documento`=?, `rep_legal_fecha_exp_documento`=?, `rep_legal_lugar_expedicion`=?, `rep_legal_fecha_nacimiento`=?, "
										+ "`rep_legal_lugar_nacimiento`=?, `rep_legal_nacionalidad_1`=?, `rep_legal_email`=?, `rep_legal_direccion_residencia`=?, "
										+ "`rep_legal_pais_residencia`=?, `rep_legal_departamento_residencia`=?, `rep_legal_ciudad_residencia`=?, `rep_legal_telefono_residencia`=?,"
										+ " `rep_legal_celular_residencia`=?, `rep_legal_persona_publica`=?, `rep_legal_recursos_publicos`=?, `rep_legal_obligaciones_tributarias`=?,"
										+ " `rep_legal_obligaciones_tributarias_indique`=?, `anexo_accionistas`=?, `anexo_sub_accionistas`=?, `ingresos`=?,"
										+ " `egresos`=?, `activos`=?, `pasivos`=?, `patrimonio`=?, `otros_ingresos`=?, `desc_otros_ingresos`=?,`tipo_moneda`=?, `anexo_preguntas_ppes`=?, "
										+ "`operaciones_moneda_extranjera`=?, `cuentas_moneda_exterior`=?, `productos_exterior`=?, `reclamaciones`=?, "
										+ "`reclamacion_anio`=?, `reclamacion_ramo`=?, `reclamacion_compania`=?, `reclamacion_valor`=?,`reclamacion_resultado`=?, `reclamacion_anio_2`=?,"
										+ " `reclamacion_ramo_2`=?, `reclamacion_compania_2`=?, `reclamacion_valor_2`=?, `reclamacion_resultado_2`=?, `chk_formulario_sarlaft`=?, "
										+ "`chk_documentos`=?, `otro`=?, `huella`=?, `firma`=?, `entrevista`=?, `verificacion`=?, `autoriza_info_fasecolda`=?, `autoriza_tratamiento`=? WHERE `cliente_sarlaft_juridico`.`cliente` =? ";
								preparedStatement = conexion.prepareStatement(sqlClienteJuridico); // for insert
								
								if (separacionDatosInsert[13].equalsIgnoreCase("")) {
									separacionDatosInsert[13] = "0";
								}
								if (separacionDatosInsert[26].equalsIgnoreCase("")) {
									separacionDatosInsert[26] = "0";
								}
								if (separacionDatosInsert[29].equalsIgnoreCase("")) {
									separacionDatosInsert[29] = "0";
								}
								if (separacionDatosInsert[30].equalsIgnoreCase("")) {
									separacionDatosInsert[30] = "0";
								}
								if (separacionDatosInsert[31].equalsIgnoreCase("")) {
									separacionDatosInsert[31] = "0";
								}
								if (separacionDatosInsert[32].equalsIgnoreCase("")) {
									separacionDatosInsert[32] = "0";
								}
								if (separacionDatosInsert[37].equalsIgnoreCase("")) {
									separacionDatosInsert[37] = "0";
								}
								if (separacionDatosInsert[39].equalsIgnoreCase("")) {
									separacionDatosInsert[39] = "0";
								}
								if (separacionDatosInsert[40].equalsIgnoreCase("")) {
									separacionDatosInsert[40] = "0";
								}
								if (separacionDatosInsert[41].equalsIgnoreCase("")) {
									separacionDatosInsert[41] = "0";
								}
								if (separacionDatosInsert[45].equalsIgnoreCase("")) {
									separacionDatosInsert[45] = "0";
								}
								if (separacionDatosInsert[57].equalsIgnoreCase("")) {
									separacionDatosInsert[57] = "0";
								}
								if (separacionDatosInsert[58].equalsIgnoreCase("")) {
									separacionDatosInsert[58] = "0";
								}
								if (separacionDatosInsert[64].equalsIgnoreCase("")) {
									separacionDatosInsert[64] = "0";
								}
								if (separacionDatosInsert[93].equalsIgnoreCase("")) {
									separacionDatosInsert[93] = "0";
								}
								if (separacionDatosInsert[94].equalsIgnoreCase("")) {
									separacionDatosInsert[94] = "0";
								}
								if (separacionDatosInsert[95].equalsIgnoreCase("")) {
									separacionDatosInsert[95] = "0";
								}
								if (separacionDatosInsert[96].equalsIgnoreCase("")) {
									separacionDatosInsert[96] = "0";
								}
								if (separacionDatosInsert[97].equalsIgnoreCase("")) {
									separacionDatosInsert[97] = "0";
								}
								if (separacionDatosInsert[98].equalsIgnoreCase("")) {
									separacionDatosInsert[98] = "0";
								}
								if (separacionDatosInsert[106].equalsIgnoreCase("")) {
									separacionDatosInsert[106] = "0";
								}
								if (separacionDatosInsert[110].equalsIgnoreCase("")) {
									separacionDatosInsert[110] = "0";
								}
								if (separacionDatosInsert[111].equalsIgnoreCase("")) {
									separacionDatosInsert[111] = "0";
								}
								if (separacionDatosInsert[114].equalsIgnoreCase("")) {
									separacionDatosInsert[114] = "0";
								}
								if (separacionDatosInsert[115].equalsIgnoreCase("")) {
									separacionDatosInsert[115] = "0";
								}
								if (separacionDatosInsert[116].equalsIgnoreCase("")) {
									separacionDatosInsert[116] = "0";
								}
								if (separacionDatosInsert[119].equalsIgnoreCase("")) {
									separacionDatosInsert[119] = "0";
								}
								if (separacionDatosInsert[120].equalsIgnoreCase("")) {
									separacionDatosInsert[120] = "0";
								}
								if (separacionDatosInsert[121].equalsIgnoreCase("")) {
									separacionDatosInsert[121] = "0";
								}
								if (separacionDatosInsert[122].equalsIgnoreCase("")) {
									separacionDatosInsert[122] = "0";
								}
								if (separacionDatosInsert[123].equalsIgnoreCase("")) {
									separacionDatosInsert[123] = "0";
								}
								if (separacionDatosInsert[127].equalsIgnoreCase("")) {
									separacionDatosInsert[127] = "0";
								}
								if (separacionDatosInsert[128].equalsIgnoreCase("")) {
									separacionDatosInsert[128] = "0";
								}  								
								preparedStatement.setInt(1, Integer.parseInt(separacionDatosInsert[13])); // ciudad_diligenciamiento
								preparedStatement.setString(2, separacionDatosInsert[14]); // sucursal
								preparedStatement.setString(3, separacionDatosInsert[15]); // tipo_solicitud
								preparedStatement.setString(4, separacionDatosInsert[16]); // residencia_sociedad
								preparedStatement.setString(5, separacionDatosInsert[17]); // clase_vinculacion
								preparedStatement.setString(6, separacionDatosInsert[18]); // clase_vinculacion_otro
								preparedStatement.setString(7, separacionDatosInsert[19]); // relacion_tom_asegurado
								preparedStatement.setString(8, separacionDatosInsert[20]); // relacion_tom_asegurado_otra
								preparedStatement.setString(9, separacionDatosInsert[21]); // relacion_tom_beneficiario
								preparedStatement.setString(10, separacionDatosInsert[22]); // relacion_tom_beneficiario_otra
								preparedStatement.setString(11, separacionDatosInsert[23]); // relacion_aseg_beneficiario
								preparedStatement.setString(12, separacionDatosInsert[24]); // relacion_aseg_beneficiario_otra
								preparedStatement.setString(13, separacionDatosInsert[25]); // razon_social
								preparedStatement.setInt(14, Integer.parseInt(separacionDatosInsert[26])); // info_basica_tipo_sociedad
								preparedStatement.setString(15, separacionDatosInsert[27]); // ofi_principal_direccion
								preparedStatement.setString(16, separacionDatosInsert[28]); // ofi_principal_tipo_empresa
								preparedStatement.setInt(17, Integer.parseInt(separacionDatosInsert[29])); // ofi_principal_departamento_empresa
								preparedStatement.setInt(18, Integer.parseInt(separacionDatosInsert[30])); // ofi_principal_ciudad_empresa
								preparedStatement.setInt(19, Integer.parseInt(separacionDatosInsert[31])); // ofi_principal_telefono
								preparedStatement.setInt(20, Integer.parseInt(separacionDatosInsert[32])); // ofi_principal_fax								
								preparedStatement.setString(21, separacionDatosInsert[33]); // ofi_principal_pagina_web
								preparedStatement.setString(22, separacionDatosInsert[34]); // ofi_principal_email
								preparedStatement.setString(23, separacionDatosInsert[35]); // ofi_principal_ciiu
								preparedStatement.setString(24, separacionDatosInsert[36]); // ofi_principal_ciiu_cod
								preparedStatement.setInt(25, Integer.parseInt(separacionDatosInsert[37])); // ofi_principal_sector
								preparedStatement.setString(26, separacionDatosInsert[38]); // sucursal_direccion
								preparedStatement.setInt(27, Integer.parseInt(separacionDatosInsert[39])); // sucursal_departamento
								preparedStatement.setInt(28, Integer.parseInt(separacionDatosInsert[40])); // sucursal_ciudad
								preparedStatement.setInt(29, Integer.parseInt(separacionDatosInsert[41])); // sucursal_telefono
								preparedStatement.setString(30, separacionDatosInsert[42]); // rep_legal_primer_apellido
								preparedStatement.setString(31, separacionDatosInsert[43]); // rep_legal_segundo_apellido
								preparedStatement.setString(32, separacionDatosInsert[44]); // rep_legal_nombres
								preparedStatement.setInt(33, Integer.parseInt(separacionDatosInsert[45])); // rep_legal_tipo_documento
								preparedStatement.setString(34, separacionDatosInsert[46]); // rep_legal_documento
								if (separacionDatosInsert[47].equalsIgnoreCase("")) {
									preparedStatement.setNull(35, java.sql.Types.DATE ); // ppes_fecha_ingreso
								}else {
									preparedStatement.setString(35, separacionDatosInsert[47]); // rep_legal_fecha_exp_documento
								}
//								preparedStatement.setString(35, separacionDatosInsert[47]); // rep_legal_fecha_exp_documento
								preparedStatement.setString(36, separacionDatosInsert[48]); // rep_legal_lugar_expedicion
								if (separacionDatosInsert[49].equalsIgnoreCase("")) {
									preparedStatement.setNull(37, java.sql.Types.DATE ); // ppes_fecha_ingreso
								}else {
									preparedStatement.setString(37, separacionDatosInsert[49]); // rep_legal_fecha_nacimiento
								}
//								preparedStatement.setString(37, separacionDatosInsert[49]); // rep_legal_fecha_nacimiento
								preparedStatement.setString(38, separacionDatosInsert[50]); // rep_legal_lugar_nacimiento
								preparedStatement.setInt(39, Integer.parseInt(separacionDatosInsert[51])); // rep_legal_nacionalidad_1
								preparedStatement.setString(40, separacionDatosInsert[52]); // rep_legal_email
								preparedStatement.setString(41, separacionDatosInsert[53]); // rep_legal_direccion_residencia
								preparedStatement.setInt(42, Integer.parseInt(separacionDatosInsert[54])); // rep_legal_pais_residencia
								preparedStatement.setInt(43, Integer.parseInt(separacionDatosInsert[55])); // rep_legal_departamento_residencia
								preparedStatement.setInt(44, Integer.parseInt(separacionDatosInsert[56])); // rep_legal_ciudad_residencia
								preparedStatement.setInt(45, Integer.parseInt(separacionDatosInsert[57])); // rep_legal_telefono_residencia
								preparedStatement.setInt(46, Integer.parseInt(separacionDatosInsert[58])); // rep_legal_celular_residencia
								preparedStatement.setString(47, separacionDatosInsert[59]); // rep_legal_persona_publica
								preparedStatement.setString(48, separacionDatosInsert[60]); // rep_legal_recursos_publicos
								preparedStatement.setString(49, separacionDatosInsert[61]); // rep_legal_obligaciones_tributarias
								preparedStatement.setString(50, separacionDatosInsert[62]); // rep_legal_obligaciones_tributarias_indique
								preparedStatement.setString(51, separacionDatosInsert[63]); // anexo_accionistas
								preparedStatement.setInt(52, Integer.parseInt(separacionDatosInsert[64])); // anexo_sub_accionistas
								preparedStatement.setInt(53, Integer.parseInt(separacionDatosInsert[93])); // ingresos
								preparedStatement.setInt(54, Integer.parseInt(separacionDatosInsert[94])); // egresos
								preparedStatement.setInt(55, Integer.parseInt(separacionDatosInsert[95])); // activos
								preparedStatement.setInt(56, Integer.parseInt(separacionDatosInsert[96])); // pasivos
								preparedStatement.setInt(57, Integer.parseInt(separacionDatosInsert[97])); // patrimonio
								preparedStatement.setInt(58, Integer.parseInt(separacionDatosInsert[98])); // otros_ingresos
								preparedStatement.setString(59, separacionDatosInsert[99]); // desc_otros_ingresos
								preparedStatement.setString(60, separacionDatosInsert[100]); // tipo_moneda
								preparedStatement.setInt(61, Integer.parseInt(separacionDatosInsert[106])); // anexo_preguntas_ppes
								preparedStatement.setString(62, separacionDatosInsert[107]); // operaciones_moneda_extranjera
								preparedStatement.setString(63, separacionDatosInsert[108]); // cuentas_moneda_exterior
								preparedStatement.setString(64, separacionDatosInsert[109]); // productos_exterior
								preparedStatement.setInt(65, Integer.parseInt(separacionDatosInsert[110])); // reclamaciones
								preparedStatement.setInt(66, Integer.parseInt(separacionDatosInsert[111])); // reclamacion_anio
								preparedStatement.setString(67, separacionDatosInsert[112]); // reclamacion_ramo
								preparedStatement.setString(68, separacionDatosInsert[113]); // reclamacion_compania
								preparedStatement.setInt(69, Integer.parseInt(separacionDatosInsert[114])); // reclamacion_valor
								preparedStatement.setInt(70, Integer.parseInt(separacionDatosInsert[115])); // reclamacion_resultado
								preparedStatement.setString(71, separacionDatosInsert[116]); // reclamacion_anio_2
								preparedStatement.setString(72, separacionDatosInsert[117]); // reclamacion_ramo_2
								preparedStatement.setString(73, separacionDatosInsert[118]); // reclamacion_compania_2
								preparedStatement.setInt(74, Integer.parseInt(separacionDatosInsert[119])); // reclamacion_valor_2
								preparedStatement.setInt(75, Integer.parseInt(separacionDatosInsert[120])); // reclamacion_resultado_2
								preparedStatement.setInt(76, Integer.parseInt(separacionDatosInsert[121])); // chk_formulario_sarlaft
								preparedStatement.setInt(77, Integer.parseInt(separacionDatosInsert[122])); // chk_documentos
								preparedStatement.setInt(78, Integer.parseInt(separacionDatosInsert[123])); // otro
								preparedStatement.setInt(79, Integer.parseInt(separacionDatosInsert[125])); // huella
								preparedStatement.setInt(80, Integer.parseInt(separacionDatosInsert[126])); // firma
								preparedStatement.setInt(81, Integer.parseInt(separacionDatosInsert[127])); // entrevista
								preparedStatement.setInt(82, Integer.parseInt(separacionDatosInsert[128])); // verificacion
								preparedStatement.setString(83, separacionDatosInsert[129]); // autoriza_info_fasecolda
								preparedStatement.setString(84, separacionDatosInsert[130]); // autoriza_tratamiento
								preparedStatement.setInt(85, idClientregistrado); // idClientregistrado
								preparedStatement.executeUpdate();
							} 
							
							 else {

									/*
									 * ************************************
									 * 
									 * = UPDATE cliente_sarlaft_natural = *
									 *
									 * *********************************
									 */

									String sqlClienteNatural = "UPDATE `cliente_sarlaft_natural` SET "
											+ "`ciudad_diligenciamiento`=?, `sucursal`=?, `tipo_solicitud`=?, `clase_vinculacion`=?, "
											+ "`clase_vinculacion_otro`=?, `relacion_tom_asegurado`=?,"
											+ "`relacion_tom_asegurado_otra`=?, `relacion_tom_beneficiario`=?,`relacion_tom_beneficiario_otra`=?, `relacion_aseg_beneficiario`=?,"
											+ " `relacion_aseg_beneficiario_otra`=?, `primer_apellido`=?, `segundo_apellido`=?, `primer_nombre`=?,"
											+ " `segundo_nombre`=?, `sexo`=?, `estado_civil`=?, `fecha_expedicion_documento`=?, `lugar_expedicion_documento`=?, `fecha_nacimiento`=?, "
											+ "`lugar_nacimiento`=?, `nacionalidad_1`=?, `ocupacion`=?, `direccion_residencia`=?, `departamento_residencia`=?, "
											+ "`ciudad_residencia`=?, `telefono`=?, `celular`=?, `correo_electronico`=?, `actividad_eco_principal`=?, `trabaja_actualmente`=?,"
											+ " `sector`=?, `tipo_actividad`=?, `cargo`=?, `empresa_donde_trabaja`=?, `departamento_empresa`=?,"
											+ " `ciudad_empresa`=?, `direccion_empresa`=?, `telefono_empresa`=?, `ingresos`=?, `egresos`=?, `activos`=?, `pasivos`=?, `patrimonio`=?, "
											+ "`otros_ingresos`=?, `desc_otros_ingresos`=?, `tipo_moneda`=?, `persona_publica`=?, `vinculo_persona_publica`=?, `productos_publicos`=?,"
											+ " `obligaciones_tributarias_otro_pais`=?, `desc_obligaciones_tributarias_otro_pais`=?, `anexo_preguntas_ppes`=?, "
											+ "`operaciones_moneda_extranjera`=?, `cuentas_moneda_exterior`=?, `productos_exterior`=?, `reclamaciones`=?, `reclamacion_anio`=?, "
											+ "`reclamacion_ramo`=?, `reclamacion_compania`=?, `reclamacion_valor`=?, `reclamacion_resultado`=?, `reclamacion_anio_2`=?, `reclamacion_ramo_2`=?,"
											+ " `reclamacion_compania_2`=?, `reclamacion_valor_2`=?, `reclamacion_resultado_2`=?, `chk_formulario_sarlaft`=?, `chk_documentos`=?, `otro`=?,"
											+ " `tipo_documento_otro`=?, `huella`=?, `firma`=?, `entrevista`=?, `verificacion`=?, `autoriza_info_fasecolda`=?, `autoriza_tratamiento` =? WHERE `cliente_sarlaft_natural`.`cliente` =?";

									preparedStatement = conexion.prepareStatement(sqlClienteNatural); // for insert
									// set values
									if (separacionDatosInsert[13].equalsIgnoreCase("")) {
										separacionDatosInsert[13] = "0";
									}
									if (separacionDatosInsert[75].equalsIgnoreCase("")) {
										separacionDatosInsert[75] = "0";
									}
									if (separacionDatosInsert[78].equalsIgnoreCase("")) {
										separacionDatosInsert[78] = "0";
									}
									if (separacionDatosInsert[79].equalsIgnoreCase("")) {
										separacionDatosInsert[79] = "0";
									}
									if (separacionDatosInsert[80].equalsIgnoreCase("")) {
										separacionDatosInsert[80] = "000000";
									}
									if (separacionDatosInsert[81].equalsIgnoreCase("")) {
										separacionDatosInsert[81] = "0";
									}
									if (separacionDatosInsert[85].equalsIgnoreCase("")) {
										separacionDatosInsert[85] = "0";
									}
									if (separacionDatosInsert[86].equalsIgnoreCase("")) {
										separacionDatosInsert[86] = "0";
									}
									if (separacionDatosInsert[89].equalsIgnoreCase("")) {
										separacionDatosInsert[89] = "0";
									}
									if (separacionDatosInsert[90].equalsIgnoreCase("")) {
										separacionDatosInsert[90] = "0";
									}
									if (separacionDatosInsert[92].equalsIgnoreCase("")) {
										separacionDatosInsert[92] = "000000";
									}
									if (separacionDatosInsert[93].equalsIgnoreCase("")) {
										separacionDatosInsert[93] = "0";
									}
									if (separacionDatosInsert[94].equalsIgnoreCase("")) {
										separacionDatosInsert[94] = "0";
									}
									if (separacionDatosInsert[95].equalsIgnoreCase("")) {
										separacionDatosInsert[95] = "0";
									}
									if (separacionDatosInsert[96].equalsIgnoreCase("")) {
										separacionDatosInsert[96] = "0";
									}
									if (separacionDatosInsert[97].equalsIgnoreCase("")) {
										separacionDatosInsert[97] = "0";
									}
									if (separacionDatosInsert[98].equalsIgnoreCase("")) {
										separacionDatosInsert[98] = "0";
									}
									if (separacionDatosInsert[106].equalsIgnoreCase("")) {
										separacionDatosInsert[106] = "0";
									}
									if (separacionDatosInsert[110].equalsIgnoreCase("")) {
										separacionDatosInsert[110] = "0";
									}
									if (separacionDatosInsert[111].equalsIgnoreCase("")) {
										separacionDatosInsert[111] = "0";
									}
									if (separacionDatosInsert[114].equalsIgnoreCase("")) {
										separacionDatosInsert[114] = "0";
									}
									if (separacionDatosInsert[115].equalsIgnoreCase("")) {
										separacionDatosInsert[115] = "0";
									}
									if (separacionDatosInsert[116].equalsIgnoreCase("")) {
										separacionDatosInsert[116] = "0";
									}
									if (separacionDatosInsert[119].equalsIgnoreCase("")) {
										separacionDatosInsert[119] = "0";
									}
									if (separacionDatosInsert[120].equalsIgnoreCase("")) {
										separacionDatosInsert[120] = "0";
									}
									if (separacionDatosInsert[121].equalsIgnoreCase("")) {
										separacionDatosInsert[121] = "0";
									}
									if (separacionDatosInsert[122].equalsIgnoreCase("")) {
										separacionDatosInsert[122] = "0";
									}
									if (separacionDatosInsert[123].equalsIgnoreCase("")) {
										separacionDatosInsert[123] = "0";
									}
									if (separacionDatosInsert[127].equalsIgnoreCase("")) {
										separacionDatosInsert[127] = "0";
									}
									if (separacionDatosInsert[128].equalsIgnoreCase("")) {
										separacionDatosInsert[128] = "0";
									}  
									
									preparedStatement.setInt(1, Integer.parseInt(separacionDatosInsert[13])); // ciudad_diligenciamiento|
									preparedStatement.setString(2, separacionDatosInsert[14]); // sucursal
									preparedStatement.setString(3, separacionDatosInsert[15]);// tipo_solicitud
									preparedStatement.setString(4, separacionDatosInsert[17]); // clase_vinculacion
									preparedStatement.setString(5, separacionDatosInsert[18]); // clase_vinculacion_otro
									preparedStatement.setString(6, separacionDatosInsert[19]); // relacion_tom_asegurado
									preparedStatement.setString(7, separacionDatosInsert[20]); // relacion_tom_asegurado_otra
									preparedStatement.setString(8, separacionDatosInsert[21]); // relacion_tom_beneficiario
									preparedStatement.setString(9, separacionDatosInsert[22]); // relacion_tom_beneficiario_otra
									preparedStatement.setString(10, separacionDatosInsert[23]); // relacion_aseg_beneficiario
									preparedStatement.setString(11, separacionDatosInsert[24]); // relacion_aseg_beneficiario_otra
									preparedStatement.setString(12, separacionDatosInsert[65]); // primer_apellido
									preparedStatement.setString(13, separacionDatosInsert[66].toString()); // segundo_apellido
									preparedStatement.setString(14, separacionDatosInsert[67].toString()); // primer_nombre
									preparedStatement.setString(15, separacionDatosInsert[68].toString()); // segundo_nombre
									preparedStatement.setString(16, separacionDatosInsert[69]); // sexo
									if (separacionDatosInsert[70].equalsIgnoreCase("")) {
										preparedStatement.setInt(17, java.sql.Types.NULL); // estado_civil
									}else {
										preparedStatement.setInt(17, Integer.parseInt(separacionDatosInsert[70])); // estado_civil
									}	
//									preparedStatement.setInt(17, Integer.parseInt(separacionDatosInsert[70])); // estado_civil
									if (separacionDatosInsert[71].equalsIgnoreCase("")) {
										preparedStatement.setNull(18, java.sql.Types.DATE ); // ppes_fecha_ingreso
									}else {
										preparedStatement.setString(18, separacionDatosInsert[71]); // fecha_expedicion_documento
									}
//									preparedStatement.setString(18, separacionDatosInsert[71]); // fecha_expedicion_documento
									preparedStatement.setString(19, separacionDatosInsert[72]); // lugar_expedicion_documento
									if (separacionDatosInsert[73].equalsIgnoreCase("")) {
										preparedStatement.setNull(20, java.sql.Types.DATE ); // ppes_fecha_ingreso
									}else {
										preparedStatement.setString(20, separacionDatosInsert[73]); // fecha_nacimiento
									}
//									preparedStatement.setString(20, separacionDatosInsert[73]); // fecha_nacimiento
									preparedStatement.setString(21, separacionDatosInsert[74]); // lugar_nacimiento
									preparedStatement.setInt(22, Integer.parseInt(separacionDatosInsert[75])); // nacionalidad_1
									preparedStatement.setString(23, separacionDatosInsert[76]); // ocupacion
									preparedStatement.setString(24, separacionDatosInsert[77]); // direccion_residencia
									preparedStatement.setInt(25, Integer.parseInt(separacionDatosInsert[78])); // departamento_residencia
									preparedStatement.setInt(26, Integer.parseInt(separacionDatosInsert[79])); // ciudad_residencia
									preparedStatement.setInt(27, Integer.parseInt(separacionDatosInsert[80])); // telefono
									preparedStatement.setLong(28, Long.parseLong(separacionDatosInsert[81])); // celular
									preparedStatement.setString(29, separacionDatosInsert[82]); // correo_electronico
									preparedStatement.setString(30, separacionDatosInsert[83]); // actividad_eco_principal
									preparedStatement.setInt(31, Integer.parseInt(separacionDatosInsert[84])); // trabaja_actualmente
									preparedStatement.setInt(32, Integer.parseInt(separacionDatosInsert[85])); // sector
									preparedStatement.setInt(33, Integer.parseInt(separacionDatosInsert[86])); // tipo_actividad
									preparedStatement.setString(34, separacionDatosInsert[87]); // cargo
									preparedStatement.setString(35, separacionDatosInsert[88]); // empresa_donde_trabaja
									preparedStatement.setInt(36, Integer.parseInt(separacionDatosInsert[89])); // departamento_empresa
									preparedStatement.setInt(37, Integer.parseInt(separacionDatosInsert[90])); // ciudad_empresa
									preparedStatement.setString(38, separacionDatosInsert[91]); // direccion_empresa
									preparedStatement.setInt(39, Integer.parseInt(separacionDatosInsert[92])); // telefono_empresa
									preparedStatement.setInt(40, Integer.parseInt(separacionDatosInsert[93])); // ingresos
									preparedStatement.setInt(41, Integer.parseInt(separacionDatosInsert[94])); // egresos
									preparedStatement.setInt(42, Integer.parseInt(separacionDatosInsert[95])); // activos
									preparedStatement.setInt(43, Integer.parseInt(separacionDatosInsert[96])); // pasivos
									preparedStatement.setInt(44, Integer.parseInt(separacionDatosInsert[97])); // patrimonio
									preparedStatement.setInt(45, Integer.parseInt(separacionDatosInsert[98])); // otros_ingresos
									preparedStatement.setString(46, separacionDatosInsert[99]); // desc_otros_ingresos
									preparedStatement.setString(47, separacionDatosInsert[100]); // tipo_moneda
									preparedStatement.setString(48, separacionDatosInsert[101]); // persona_publica
									preparedStatement.setString(49, separacionDatosInsert[102]); // vinculo_persona_publica
									preparedStatement.setString(50, separacionDatosInsert[103]); // productos_publicos
									preparedStatement.setString(51, separacionDatosInsert[104]); // obligaciones_tributarias_otro_pais
									preparedStatement.setString(52, separacionDatosInsert[105]); // desc_obligaciones_tributarias_otro_pais
									preparedStatement.setInt(53, Integer.parseInt(separacionDatosInsert[106])); // anexo_preguntas_ppes
									preparedStatement.setString(54, separacionDatosInsert[107]); // operaciones_moneda_extranjera
									preparedStatement.setString(55, separacionDatosInsert[108]); // cuentas_moneda_exterior
									preparedStatement.setString(56, separacionDatosInsert[109]); // productos_exterior
									preparedStatement.setInt(57, Integer.parseInt(separacionDatosInsert[110])); // reclamaciones
									preparedStatement.setInt(58, Integer.parseInt(separacionDatosInsert[111])); // reclamacion_anio
									preparedStatement.setString(59, separacionDatosInsert[112]); // reclamacion_ramo
									preparedStatement.setString(60, separacionDatosInsert[113]); // reclamacion_compania
									preparedStatement.setString(61, separacionDatosInsert[114]); // reclamacion_valor x
									preparedStatement.setInt(62, Integer.parseInt(separacionDatosInsert[115])); // reclamacion_resultado
									preparedStatement.setInt(63, Integer.parseInt(separacionDatosInsert[116])); // reclamacion_anio_2
									preparedStatement.setString(64, separacionDatosInsert[117]); // reclamacion_ramo_2
									preparedStatement.setString(65, separacionDatosInsert[118]); // reclamacion_compania_2
									preparedStatement.setInt(66, Integer.parseInt(separacionDatosInsert[119])); // reclamacion_valor_2
									preparedStatement.setInt(67, Integer.parseInt(separacionDatosInsert[120])); // reclamacion_resultado_2
									preparedStatement.setInt(68, Integer.parseInt(separacionDatosInsert[121])); // chk_formulario_sarlaft
									preparedStatement.setInt(69, Integer.parseInt(separacionDatosInsert[122])); // chk_documentos
									preparedStatement.setInt(70, Integer.parseInt(separacionDatosInsert[123])); // otro
									preparedStatement.setString(71, separacionDatosInsert[124]); // tipo_documento_otro
									preparedStatement.setInt(72, Integer.parseInt(separacionDatosInsert[125])); // huella
									preparedStatement.setInt(73, Integer.parseInt(separacionDatosInsert[126])); // firma
									preparedStatement.setInt(74, Integer.parseInt(separacionDatosInsert[127])); // entrevista
									preparedStatement.setInt(75, Integer.parseInt(separacionDatosInsert[128])); // verificacion
									preparedStatement.setString(76, separacionDatosInsert[129]); // autoriza_info_fasecolda
									preparedStatement.setString(77, separacionDatosInsert[130]); // autoriza_tratamiento
									preparedStatement.setInt(78, idClientregistrado); // idClientregistrado
									preparedStatement.executeUpdate();
								}						
						}
						
					}
					if (separacionDatosInsert[0].equals("R2")) {

						if (documentoRepetido == false) {
							
							/* ***************************************************
							 * 
							 * = INSERT TABLA relacion_archivo_radicacion (R2) = *
							 *
							 * ************************************************ */
							
							String sqlArchivoOrganizado = "INSERT INTO `asistemyca_zurich`.`relacion_archivo_radicacion` (`RADICACION_ID`, `CLIENTE_ID`, `NOMBRE_ARCHIVO`) "
									+ "VALUES (?,?,?)";

							preparedStatement = conexion.prepareStatement(sqlArchivoOrganizado); // for insert

							preparedStatement.setInt(1, idRadicacion); // RADICACION_ID
							preparedStatement.setInt(2, idClient); // CLIENTE_ID
							preparedStatement.setString(3, separacionDatosInsert[2]); // NOMBRE_ARCHIVO

							preparedStatement.executeUpdate();
						} else {
							
							/* ***************************************************
							 * 
							 * = UPDATE TABLA relacion_archivo_radicacion (R2) = *
							 *
							 * ************************************************ */
							
							String sqlUpdateArchivoOrganizado = "UPDATE `relacion_archivo_radicacion` SET `NOMBRE_ARCHIVO` =? where `relacion_archivo_radicacion`.`CLIENTE_ID` = ?)";

							preparedStatement = conexion.prepareStatement(sqlUpdateArchivoOrganizado); // for insert
							preparedStatement.setString(1, separacionDatosInsert[2]); // NOMBRE_ARCHIVO
							preparedStatement.setInt(2, idClientregistrado); // idClientregistrado

							preparedStatement.executeUpdate();
						}

					}

					if (clienteIdentCompleta == 3 || clienteIdentCompleta == 9) {

						if (separacionDatosInsert[0].equals("R3")) {

							if (documentoRepetido == false) {

								/*
								 * ********************************
								 * 
								 * = INSERT TABLA accionistas (R3) = *
								 *
								 * *****************************
								 */

								String sqlAccionistas = "INSERT INTO `asistemyca_zurich`.`accionistas` (`cliente_id`,`accionista_tipo_documento`, `accionista_documento`, `accionista_nombres_completos`, `accionista_participacion`, `accionista_cotiza_bolsa`, `accionista_persona_publica`, `accionista_obligaciones_otro_pais`, `accionista_obligaciones_otro_pais_desc`, `created`) "
										+ "VALUES (?,?,?,?,CAST(? AS DECIMAL(20,2)),?,?,?,?,?)";

								preparedStatement = conexion.prepareStatement(sqlAccionistas); // for insert

								if (separacionDatosInsert[1].equalsIgnoreCase("")) {
									separacionDatosInsert[1] = "0";
								}
								if (separacionDatosInsert[4].equalsIgnoreCase("")) {
									separacionDatosInsert[4] = "0";
								}
								preparedStatement.setInt(1, idClient); // cliente_id
								preparedStatement.setInt(2, Integer.parseInt(separacionDatosInsert[1])); // accionista_tipo_documento
								preparedStatement.setString(3, separacionDatosInsert[2]); // accionista_documento
								preparedStatement.setString(4, separacionDatosInsert[3]); // accionista_nombres_completos
								preparedStatement.setString(5, separacionDatosInsert[4]); // accionista_participacion
								preparedStatement.setString(6, separacionDatosInsert[5]);// accionista_cotiza_bolsa
								preparedStatement.setString(7, separacionDatosInsert[6]); // accionista_persona_publica
								preparedStatement.setString(8, separacionDatosInsert[7]); // accionista_obligaciones_otro_pais
								preparedStatement.setString(9, separacionDatosInsert[8]); // accionista_obligaciones_otro_pais_desc
								preparedStatement.setString(10, dateFormat.format(date).toString()); // created
								preparedStatement.executeUpdate();
							} else {

								/*
								 * ********************************
								 * 
								 * = UPDATE TABLA accionistas (R3)= *
								 *
								 * *****************************
								 */

		

								String sqlUpdateAccionistas = "UPDATE `accionistas` SET `accionista_tipo_documento` =?, `accionista_documento` =?,"
										+ " `accionista_nombres_completos` =?, `accionista_participacion` =?, `accionista_cotiza_bolsa` =?,"
										+ "`accionista_persona_publica` =?, `accionista_obligaciones_otro_pais` =?, `accionista_obligaciones_otro_pais_desc`=? where `accionistas`.`cliente_id` = ?";

								preparedStatement = conexion.prepareStatement(sqlUpdateAccionistas); // for insert
								if (separacionDatosInsert[1].equalsIgnoreCase("")) {
									separacionDatosInsert[1] = "0";
								}
								if (separacionDatosInsert[4].equalsIgnoreCase("")) {
									separacionDatosInsert[4] = "0";
								}
								preparedStatement.setInt(1, Integer.parseInt(separacionDatosInsert[1])); // accionista_tipo_documento
								preparedStatement.setString(2, separacionDatosInsert[2]); // accionista_documento
								preparedStatement.setString(3, separacionDatosInsert[3]); // accionista_nombres_completos
								preparedStatement.setString(4, separacionDatosInsert[4]); // accionista_participacion
								preparedStatement.setString(5, separacionDatosInsert[5]);// accionista_cotiza_bolsa
								preparedStatement.setString(6, separacionDatosInsert[6]); // accionista_persona_publica
								preparedStatement.setString(7, separacionDatosInsert[7]); // accionista_obligaciones_otro_pais
								preparedStatement.setString(8, separacionDatosInsert[8]); // accionista_obligaciones_otro_pais_desc
								preparedStatement.setInt(9, idClientregistrado); // idClientregistrado
								preparedStatement.executeUpdate();
							}
						}
						if (separacionDatosInsert[0].equals("R4")) {

							if (documentoRepetido == false) {
								
								/*
								 * ********************************
								 * 
								 * = INSERT TABLA sub_accionistas (R4) = *
								 *
								 * *****************************
								 */

								String sqlSubAccionistas = "INSERT INTO `asistemyca_zurich`.`sub_accionistas` (`cliente_id`, `sub_accionista_tipo_documento`, `sub_accionista_numero_id`, `sub_accionista_razon_social`, `sub_accionista_participacion`, `sub_accionista_nombre_sociedad_accionista`, `sub_accionista_documento`, `created`) "
										+ "VALUES (?,?,?,?,CAST(? AS DECIMAL(20,2)),?,?,?)";

								preparedStatement = conexion.prepareStatement(sqlSubAccionistas); // for insert

								if (separacionDatosInsert[1].equalsIgnoreCase("")) {
									separacionDatosInsert[1] = "0";
								}
								if (separacionDatosInsert[4].equalsIgnoreCase("")) {
									separacionDatosInsert[4] = "0";
								}
								preparedStatement.setInt(1, idClient); // cliente_id
								preparedStatement.setInt(2, Integer.parseInt(separacionDatosInsert[1])); // sub_accionista_tipo_documento
								preparedStatement.setString(3, separacionDatosInsert[2]); // sub_accionista_numero_id
								preparedStatement.setString(4, separacionDatosInsert[3]); // sub_accionista_razon_social
								preparedStatement.setString(5, separacionDatosInsert[4]); // sub_accionista_participacion
								preparedStatement.setString(6, separacionDatosInsert[5]);// sub_accionista_nombre_sociedad_accionista
								preparedStatement.setString(7, separacionDatosInsert[6]); // sub_accionista_documento
								preparedStatement.setString(8, dateFormat.format(date).toString()); // created
								preparedStatement.executeUpdate();
							} else {
								
								/*
								 * ********************************
								 * 
								 * = UPDATE TABLA sub_accionistas (R4) = *
								 *
								 * *****************************
								 */

								String sqlUpdateSubAccionistas = "UPDATE `sub_accionistas` SET `sub_accionista_tipo_documento` =?, "
										+ "`sub_accionista_numero_id` =?, `sub_accionista_razon_social` =?, `sub_accionista_participacion` =?, "
										+ "`sub_accionista_nombre_sociedad_accionista` =?, `sub_accionista_documento` =?  where `sub_accionistas`.`cliente_id` = ?";

								preparedStatement = conexion.prepareStatement(sqlUpdateSubAccionistas); // for insert
								if (separacionDatosInsert[1].equalsIgnoreCase("")) {
									separacionDatosInsert[1] = "0";
								}
								if (separacionDatosInsert[4].equalsIgnoreCase("")) {
									separacionDatosInsert[4] = "0";
								}
								preparedStatement.setInt(1, Integer.parseInt(separacionDatosInsert[1])); // sub_accionista_tipo_documento
								preparedStatement.setString(2, separacionDatosInsert[2]); // sub_accionista_numero_id
								preparedStatement.setString(3, separacionDatosInsert[3]); // sub_accionista_razon_social
								preparedStatement.setString(4, separacionDatosInsert[4]); // sub_accionista_participacion
								preparedStatement.setString(5, separacionDatosInsert[5]);// sub_accionista_nombre_sociedad_accionista
								preparedStatement.setString(6, separacionDatosInsert[6]); // sub_accionista_documento
								preparedStatement.setInt(7, idClientregistrado); // idClientregistrado
								preparedStatement.executeUpdate();
							}
						}
					}

					if (separacionDatosInsert[0].equals("R5")) {

						/*
						 * ***********************************
						 * 
						 * = INSERT R5 zr_anexos_ppes (R5) = *
						 *
						 * ***********************************
						 */

						if (documentoRepetido == false) {

							String sqlAnexosPpes = "INSERT INTO `asistemyca_zurich`.`zr_anexos_ppes` ("
									+ "`cliente_id`, `ppes_vinculo_relacion`, `ppes_nombre`, `ppes_tipo_identificacion`, `ppes_no_documento`, `ppes_nacionalidad`, "
									+ "`ppes_entidad`, `ppes_cargo`, `ppes_fecha_ingreso` , `ppes_desvinculacion`, `ppes_motivo` ,`created`)"
									+ " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

							preparedStatement = conexion.prepareStatement(sqlAnexosPpes); // for insert
							if (separacionDatosInsert[3].equalsIgnoreCase("")) {
								separacionDatosInsert[3] = "0";
							}
							if (separacionDatosInsert[5].equalsIgnoreCase("")) {
								separacionDatosInsert[5] = "0";
							}
							preparedStatement.setInt(1, idClient); // cliente_id
							preparedStatement.setString(2, separacionDatosInsert[1]); // ppes_vinculo_relacion
							preparedStatement.setString(3, separacionDatosInsert[2]); // ppes_nombre
							preparedStatement.setInt(4, Integer.parseInt(separacionDatosInsert[3])); // ppes_tipo_identificacion
							preparedStatement.setString(5, separacionDatosInsert[4]); // ppes_no_documento
							preparedStatement.setInt(6, Integer.parseInt(separacionDatosInsert[5]));// ppes_nacionalidad
							preparedStatement.setString(7, separacionDatosInsert[6]); // ppes_entidad
							preparedStatement.setString(8, separacionDatosInsert[7]); // ppes_cargo
							if (separacionDatosInsert[8].equalsIgnoreCase("")) {
								preparedStatement.setNull(9, java.sql.Types.DATE ); // ppes_fecha_ingreso
							}else {
								preparedStatement.setString(9, separacionDatosInsert[8]); // ppes_fecha_ingreso
							}
							if (separacionDatosInsert[9].equalsIgnoreCase("")) {
								preparedStatement.setNull(10, java.sql.Types.DATE ); // ppes_fecha_ingreso
							}else {
								preparedStatement.setString(10, separacionDatosInsert[9]); // ppes_desvinculacion
							}	
							preparedStatement.setString(11, separacionDatosInsert[10]); // ppes_motivo
							preparedStatement.setString(12, dateFormat.format(date).toString()); // created
							preparedStatement.executeUpdate();
						} else {

							/*
							 * *****************************
							 * 
							 * = UPDATE R5 zr_anexos_ppes (R5) =*
							 *
							 * **************************
							 */
							
							String sqlUpdateZr_anexos_ppes = "UPDATE `zr_anexos_ppes` SET `ppes_vinculo_relacion` =?, `ppes_nombre` =?, `ppes_tipo_identificacion` =?,"
									+ " `ppes_no_documento` =?, `ppes_nacionalidad` =?, `ppes_entidad` =?, `ppes_cargo` =?, ppes_fecha_ingreso =?, `ppes_desvinculacion` =?, `ppes_motivo` = ? where `zr_anexos_ppes`.`cliente_id` = ?";
							preparedStatement = conexion.prepareStatement(sqlUpdateZr_anexos_ppes); // for insert
							
							if (separacionDatosInsert[3].equalsIgnoreCase("")) {
								separacionDatosInsert[3] = "0";
							}
							if (separacionDatosInsert[5].equalsIgnoreCase("")) {
								separacionDatosInsert[5] = "0";
							}
							preparedStatement.setString(1, separacionDatosInsert[1]); // ppes_vinculo_relacion
							preparedStatement.setString(2, separacionDatosInsert[2]); // ppes_nombre
							preparedStatement.setInt(3, Integer.parseInt(separacionDatosInsert[3])); // ppes_tipo_identificacion
							preparedStatement.setString(4, separacionDatosInsert[4]); // ppes_no_documento
							preparedStatement.setInt(5, Integer.parseInt(separacionDatosInsert[5]));// ppes_nacionalidad
							preparedStatement.setString(6, separacionDatosInsert[6]); // ppes_entidad
							preparedStatement.setString(7, separacionDatosInsert[7]); // ppes_cargo
							if (separacionDatosInsert[8].equalsIgnoreCase("")) {
								preparedStatement.setNull(8, java.sql.Types.DATE ); // ppes_fecha_ingreso
							}else {
								preparedStatement.setString(8, separacionDatosInsert[8]); // ppes_fecha_ingreso
							}
							if (separacionDatosInsert[9].equalsIgnoreCase("")) {
								preparedStatement.setNull(9, java.sql.Types.DATE ); // ppes_fecha_ingreso
							}else {
								preparedStatement.setString(9, separacionDatosInsert[9]); // ppes_desvinculacion
							}							
							preparedStatement.setString(10, separacionDatosInsert[10]); // ppes_motivo
							preparedStatement.setInt(11, idClientregistrado); // idClientregistrado
							preparedStatement.executeUpdate();
						}
					}
				} catch (Exception ex) {
					conexion.rollback();
					error = "error en los datos recibidos. Por favor revise la linea  " + '\n' + separacionDatos[i];
					conexion.close();
					System.out.println("cat1 " + ex.getMessage());
					SendMail.enviarcorreo(error, nomArchivo, correoDestinatario.getCorreo());
					return false;
				}
			}
			// End of JDBC transaction
			conexion.commit(); // commit, if successful
			System.out.println("commit done !!");
			conexion.close();
			System.out.println(correoDestinatario.getCorreo());
			return true;

		} catch (Exception e) {
			error = "se ha producido un error al procesar el archivo. Por favor revise";
			System.out.print(error);
			conexion.rollback();
			conexion.close();
			System.out.println("ca2" + e.getMessage());
			SendMail.enviarcorreo(error, nomArchivo, correoDestinatario.getCorreo());
			return false;

		}

	}

	public static boolean verificacionArchivoBD(String nomArchivo) {
		int resultado = 0;
		PreparedStatement preparedStatement = null;
		// instanciamos la clase Connection la cual representa la conexión con la Base
		// de Datos
		Connection conexion;
		ConexionBD miConexion = new ConexionBD();
		// asignamos la conexión a nuesta BD
		conexion = miConexion.getConnection();
		ResultSet rs;

		try {
			conexion.setAutoCommit(true);
			preparedStatement = conexion.prepareStatement(
					"SELECT COUNT(*) FROM asistemyca_zurich.relacion_archivo_radicacion where NOMBRE_ARCHIVO = '"
							+ nomArchivo + "'");
			preparedStatement.executeQuery();
			rs = preparedStatement.getResultSet();
			if (rs.next()) {
				resultado = rs.getInt(1);
			}
			conexion.close();
			if (resultado > 0) {
				return true;
			} else {
				return false;
			}

		} catch (Exception e) {
			System.out.println("No hay ficheros en el directorio especificado");
			return false;
		}

	}

	public String getCorreo() {
		return correo;
	}

	public void setCorreo(String correo) {
		this.correo = correo;
	}

	public void validacionR3() {

	}

}
