package co.com.zurich.config;

import java.io.File;
import java.io.FileInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.jasypt.encryption.pbe.PBEStringEncryptor;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

public class ArchivoPropiedades {
	public static Properties propiedades;
	public static String valor = "A54551.";

	public ArchivoPropiedades() {
	}

	public static Properties getPropiedades() {
		if (propiedades == null) {
			// propiedades = new Properties();
			InputStream entrada = null;

			try {
				/*
				 * Instanciamos el objeto de encriptación
				 */
				PBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
				encryptor.setPassword(valor);

				/*
				 * Instanciamos un objeto de la clase EncryptableProperties de Jasypt, que
				 * extiende de Properties. Esta clase permite leer las propiedades encriptadas,
				 * ya que utiliza el encriptador para desencriptarlas y devolvernos el valor en
				 * claro.
				 */
				propiedades = new EncryptableProperties(encryptor);
				InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream("ZurichData.properties");
				propiedades.load(in);

				/*
				 * Antiguo: borrar cuando funcione el esquema cifrado entrada = new
				 * FileInputStream("zurich.properties");
				 * 
				 * // cargamos el archivo de propiedades propiedades.load(entrada);
				 */

			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				if (entrada != null) {
					try {
						entrada.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return propiedades;
	}

}
