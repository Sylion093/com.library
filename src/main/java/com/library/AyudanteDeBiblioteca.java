package com.library;

import java.awt.EventQueue;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AyudanteDeBiblioteca {

	private JFrame frmAyudanteDeBiblioteca;
	private File[] archivosSeleccionados;
	private JTextPane tpResultados;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					AyudanteDeBiblioteca window = new AyudanteDeBiblioteca();
					window.frmAyudanteDeBiblioteca.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public AyudanteDeBiblioteca() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmAyudanteDeBiblioteca = new JFrame();
		frmAyudanteDeBiblioteca.setTitle("Ayudante de Biblioteca");
		frmAyudanteDeBiblioteca.setBounds(100, 100, 450, 300);
		frmAyudanteDeBiblioteca.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		JSplitPane splitPane = new JSplitPane();
		splitPane.setResizeWeight(0.4);
		frmAyudanteDeBiblioteca.getContentPane().add(splitPane, BorderLayout.SOUTH);
		
		JButton btnSeleccionarArchivos = new JButton("Seleccionar Archivos");
		btnSeleccionarArchivos.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                seleccionarArchivos();
            }
        });
		splitPane.setLeftComponent(btnSeleccionarArchivos);
		
		JButton btnCopiarYClasificar = new JButton("Subir Archivos");
		btnCopiarYClasificar.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                copiarArchivosYClasificar();
            }
        });
		splitPane.setRightComponent(btnCopiarYClasificar);
		
		tpResultados = new JTextPane();
		frmAyudanteDeBiblioteca.getContentPane().add(tpResultados, BorderLayout.CENTER);
	}
	
	private void logResultado(String mensaje) {
        tpResultados.setText(tpResultados.getText() + mensaje + "\n");
    }
	
	private void seleccionarArchivos() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setMultiSelectionEnabled(true);
        fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
        int result = fileChooser.showOpenDialog(fileChooser);
        if (result == JFileChooser.APPROVE_OPTION) {
            archivosSeleccionados = fileChooser.getSelectedFiles();
            logResultado("Archivos seleccionados: " + archivosSeleccionados.length);
        }
    }

    private void copiarArchivosYClasificar() {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            //Eliminar automáticamente la carpeta /biblioteca_temp antes de copiar los archivos
            Path carpetaTemp = new Path("/biblioteca_temp");
            if (fs.exists(carpetaTemp)) {
                fs.delete(carpetaTemp, true);
                logResultado("Carpeta /biblioteca_temp eliminada en HDFS.");
            }

            // Verificar si hay archivos seleccionados
            if (archivosSeleccionados != null && archivosSeleccionados.length > 0) {
                Path destino = new Path("/libros");
                for (File archivo : archivosSeleccionados) {
                    Path destinoArchivo = new Path(destino, archivo.getName());
                    fs.copyFromLocalFile(new Path(archivo.getAbsolutePath()), destinoArchivo);
                }
                logResultado("Archivos copiados a HDFS en /libros.");
            } else {
            	logResultado("No se seleccionaron archivos para copiar.");
                return; // Termina el método si no hay archivos seleccionados
            }

            // Ejecutar ClasificarLibrosPorTema desde el mismo JAR
            ejecutarClasificarLibros();

        } catch (Exception ex) {
        	logResultado("Error durante el proceso: " + ex.getMessage());
        }
    }

    private void ejecutarClasificarLibros() {
        try {
            // Configurar el comando para ejecutar el trabajo de MapReduce
            /* ProcessBuilder builder = new ProcessBuilder(
                "hadoop", "jar", "clasificar_libros.jar", "com.example.ClasificarLibrosPorTema", "/libros", "/biblioteca"
            );
            Process process = builder.start();
            process.waitFor();*/
        	ClasificarLibrosPorTema clasificarLibros = new ClasificarLibrosPorTema();
        	clasificarLibros.run();
        	logResultado("Clasificación de libros completada.");
        } catch (Exception ex) {
        	logResultado("Error al ejecutar ClasificarLibrosPorTema: " + ex.getMessage());
        }
    }

}
