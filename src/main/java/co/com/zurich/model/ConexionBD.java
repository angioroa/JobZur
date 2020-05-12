package co.com.zurich.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import co.com.zurich.config.ArchivoPropiedades;


public class ConexionBD {

	@SuppressWarnings("finally")
	public static Connection getConnection() {

		Connection conexion = null;
		try {
			//Class.forName("com.mysql.jdbc.Driver");
			Class.forName("com.mysql.cj.jdbc.Driver");
			
			//asignamos a variables, el contenido de las variables que tenemos en nuestro .properties
			String ip = ArchivoPropiedades.getPropiedades().getProperty("ip_base_datos");
			String nombreDB = ArchivoPropiedades.getPropiedades().getProperty("nombre_base_datos");
			String usuarioDB = ArchivoPropiedades.getPropiedades().getProperty("usuarioDB");
			String puerto = ArchivoPropiedades.getPropiedades().getProperty("port");

			//Unificicamos toda nuestra ruta en la cual esta la conexión a la BD
			String servidor = "jdbc:mysql://"+ip +":"+puerto+ "/"+ nombreDB +"?useSSL=false && serverTimezone=UTC";

			//Realizamos la conexión completa, se le agrega ruta del servidor,usuarioBD y claveBD si se tiene 
			conexion= DriverManager.getConnection(servidor,usuarioDB,"");


		} catch(ClassNotFoundException ex)
		{
			ex.printStackTrace();
			conexion=null;
		}
		catch(SQLException ex)
		{
			ex.printStackTrace();
			conexion=null;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			conexion=null;
		}

		finally
		{
			return conexion;
		}
	}

}
