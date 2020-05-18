package co.com.zurich.controller;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import co.com.zurich.config.ArchivoPropiedades;
import co.com.zurich.model.QuerysBD;

public class ReadFile {

	//Se asigna a nuestra variable, el contenido de la ruta establecida en .properties	
		String ruta_archivo = ArchivoPropiedades.getPropiedades().getProperty("route_file");
		String archivos_procesados = ArchivoPropiedades.getPropiedades().getProperty("procesados");
		String archivos_error = ArchivoPropiedades.getPropiedades().getProperty("errores");
		String pendiente_radicar = ArchivoPropiedades.getPropiedades().getProperty("radicar");
		String iniciPdf = ArchivoPropiedades.getPropiedades().getProperty("iniciPdf");
		String AlmacenamientoPdf = ArchivoPropiedades.getPropiedades().getProperty("AlmacenamientoPdf");

		public void listFilePdf() throws Throwable {
			File dir = new File(iniciPdf);
			String[] ficheros = dir.list();
			
			if (ficheros.length <= 0) {
				System.out.println("No hay archivos PDF en el directorio especificado");
			}else {
				for (int x = 0; x < ficheros.length; x++) {
					String rutaCompleta = "";
					rutaCompleta = iniciPdf +"\\" + ficheros[x];
					if (ficheros[x].endsWith(".pdf") || ficheros[x].endsWith(".PDF")) //selecciona solo los archivox pdf
					{
						System.out.println(ficheros[x]);
						readFilePdf(rutaCompleta,ficheros[x]);
					}
				}
			}
		}


	public void listFiles() throws Throwable {
		try {
			//Clase que nos permite leer o escribir, le asignamos la ruta en la cual se encuentra
			//el archivo a leer

			//FileReader fr = new FileReader(ruta_archivo);

			File dir = new File(ruta_archivo);
			String[] ficheros = dir.list();

			File directorio = new File(archivos_procesados);
			directorio.mkdirs();
			
			File directorioError = new File(archivos_error);
			directorioError.mkdirs();
			
			if (ficheros.length <= 0) {
				System.out.println("No hay ficheros en el directorio especificado");
			}else { 

				for (int x=0;x<ficheros.length;x++) {


					String rutaCompleta = "";
					rutaCompleta = ruta_archivo +"\\" + ficheros[x];
					if (ficheros[x].endsWith(".txt") || ficheros[x].endsWith(".TXT")) //selecciona solo los archivox txt
					{
						System.out.println(ficheros[x]);

						readFile(rutaCompleta,ficheros[x]);
					}
				}

				dir = new File(ruta_archivo);
				ficheros = dir.list();
				for (int i = 0; i < ficheros.length; i++) {
					String rutaCompleta = "";
					rutaCompleta = ruta_archivo +"\\" + ficheros[i];
					verificacionArchivo(ficheros[i],rutaCompleta);
				}

			}

		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void readFile(String ruta, String nomArchivo) throws Throwable {
		String cadena;
		String aux = "";
		try {
			FileReader reader = new FileReader(ruta);
			BufferedReader bf = new BufferedReader(reader);
 
			//Recorremos en un while todas las lineas una a una hasta que llegue a una vacia
			while ((cadena = bf.readLine())!=null) {
				//asignamos una variable temporal, en la cual vamos a guardar todas las lineas
				//y le concatenamos un salto de linea para hacer mas facil el .split
				aux = aux + cadena + "\n";	
			}
			reader.close();
			//metodo en cual enviamos todo el temporal con todas las lineas de nuestro txt
			if (QuerysBD.dataCompleta(aux,nomArchivo)) {
				//metodo de envio de correo
				//QuerysBD correo = new QuerysBD();
//				System.out.println(Destinatario.getCorreo());
				SendMail.enviarcorreo(null, nomArchivo,Destinatario.getCorreo());
				//System.out.println(correo.getCorreo());
				System.out.println("Archivo procesado corectamente.");
				rutas(ruta, archivos_procesados);

			} else {
				System.out.println("Se ha producido un error.");
				rutas(ruta, archivos_error);

			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void readFilePdf(String ruta, String nomArchivo) throws IOException {
		System.out.println("procesando PDF.");
		rutas(ruta, AlmacenamientoPdf );
		System.out.println("PDF procesado corectamente.");
	}
	
	public static void rutas (String orig, String desc) throws IOException {
		try {
			Path origen = Paths.get(orig);
			Path destino = Paths.get(desc);
			Files.move(origen, destino.resolve(origen.getFileName()), StandardCopyOption.REPLACE_EXISTING);

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

	}

	public  void verificacionArchivo(String archivo, String rutaCompleta) {
		try {
			if (QuerysBD.verificacionArchivoBD(archivo)) {
				rutas(rutaCompleta, pendiente_radicar);
			}else {
				rutas(rutaCompleta, archivos_error);
			} 
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

}

