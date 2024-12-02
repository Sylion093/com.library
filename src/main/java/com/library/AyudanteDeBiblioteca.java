package com.library;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class AyudanteDeBiblioteca {

    private JFrame frmAyudanteDeBiblioteca;
    private JTextPane tpResultados;
    private JTable taChat;
    private JTextField tfMensaje;
    private DefaultTableModel dtm = new DefaultTableModel(0,0);

    private File[] archivosSeleccionados;

    public static void main(String[] args) {
        EventQueue.invokeLater(() -> {
            try {
                AyudanteDeBiblioteca window = new AyudanteDeBiblioteca();
                window.frmAyudanteDeBiblioteca.setVisible(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public AyudanteDeBiblioteca() {
        initialize();
    }

    private void initialize() {
        frmAyudanteDeBiblioteca = new JFrame();
        frmAyudanteDeBiblioteca.setTitle("Ayudante de Biblioteca");
        frmAyudanteDeBiblioteca.setBounds(100, 100, 800, 600);
        frmAyudanteDeBiblioteca.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        // Configurar pestañas
        JTabbedPane tabbedPane = new JTabbedPane();
        frmAyudanteDeBiblioteca.getContentPane().add(tabbedPane, BorderLayout.CENTER);

        // Primera pestaña: Gestión de Archivos
        JPanel panelArchivos = new JPanel(new BorderLayout());

        tpResultados = new JTextPane();
        tpResultados.setEditable(false);
        panelArchivos.add(new JScrollPane(tpResultados), BorderLayout.CENTER);

        JPanel panelBotones = new JPanel(new GridLayout(1, 2));
        JButton btnSeleccionarArchivos = new JButton("Seleccionar Archivos");
        btnSeleccionarArchivos.addActionListener(e -> seleccionarArchivos());
        panelBotones.add(btnSeleccionarArchivos);

        JButton btnCopiarYClasificar = new JButton("Subir y Clasificar");
        btnCopiarYClasificar.addActionListener(e -> realizarClasificacion());
        panelBotones.add(btnCopiarYClasificar);

        panelArchivos.add(panelBotones, BorderLayout.SOUTH);
        tabbedPane.addTab("Gestión de Archivos", panelArchivos);

        // Segunda pestaña: Chat
        JPanel panelChat = new JPanel(new BorderLayout());

        taChat = new JTable();
        dtm.addColumn("Mensajes");
        dtm.addColumn("Descarga");
        dtm.addColumn("Like");        

        taChat.setModel(dtm);
        
        panelChat.add(new JScrollPane(taChat), BorderLayout.CENTER);

        tfMensaje = new JTextField();
        tfMensaje.addActionListener(e -> enviarMensaje());
        panelChat.add(tfMensaje, BorderLayout.SOUTH);

        tabbedPane.addTab("Chat", panelChat);
    }

    private void logResultado(String mensaje) {
        //tpResultados.setText(tpResultados.getText() + mensaje + "\n");
        dtm.addRow(new Object[] {mensaje,"",""});
    }
    private void logRespuesta(String titulo, String Ruta){
    	dtm.addRow(new Object[] {});
    }

    private void seleccionarArchivos() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setMultiSelectionEnabled(true);
        int result = fileChooser.showOpenDialog(frmAyudanteDeBiblioteca);
        if (result == JFileChooser.APPROVE_OPTION) {
            archivosSeleccionados = fileChooser.getSelectedFiles();
            logResultado("Archivos seleccionados: " + archivosSeleccionados.length);
        }
    }

    private void realizarClasificacion() {
        new Thread(() -> {
            try {
                logResultado("Iniciando clasificación...");

                // Subir archivos seleccionados a HDFS
                Path hdfsLibros = new Path("/libros");
                subirArchivosAHDFS(hdfsLibros);

                // Ejecutar la clasificación
                boolean jobSuccess = ClasificarLibrosPorTema.runJob(new String[] {"/libros", "/biblioteca_temp"});
                if (jobSuccess) {
                    logResultado("Clasificación completada.");
                } else {
                    logResultado("Error en la clasificación.");
                }

            } catch (Exception e) {
                logResultado("Error durante la clasificación: " + e.getMessage());
            }
        }).start();
    }

    private void subirArchivosAHDFS(Path destinoHDFS) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Crear directorio en HDFS si no existe
        if (!fs.exists(destinoHDFS)) {
            fs.mkdirs(destinoHDFS);
        }

        // Copiar archivos seleccionados
        for (File archivo : archivosSeleccionados) {
            Path destinoArchivo = new Path(destinoHDFS, archivo.getName());
            fs.copyFromLocalFile(new Path(archivo.getAbsolutePath()), destinoArchivo);
            logResultado("Archivo copiado a HDFS: " + archivo.getName());
        }
    }

    private void enviarMensaje() {
    	String mensaje = tfMensaje.getText();
        String respuesta;
        // taChat.append("Usuario: " + mensaje + "\n");
        

        try {
            // Define rutas de entrada y salida en HDFS
            String inputPath = "/biblioteca";
            String outputPath = "/biblioteca/output";

            // Verifica y elimina el directorio de salida si existe
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path outputDir = new Path(outputPath);
            if (fs.exists(outputDir)) {
                fs.delete(outputDir, true);
            }

            // Recolectar rutas de archivos PDF
            List<String> pdfPaths = new ArrayList<>();
            Path inputDir = new Path(inputPath);
            FileStatus[] categoryDirs = fs.listStatus(inputDir);

            for (FileStatus categoryDir : categoryDirs) {
                if (categoryDir.isDirectory()) {
                    FileStatus[] pdfFiles = fs.listStatus(categoryDir.getPath());
                    for (FileStatus pdfFile : pdfFiles) {
                        if (pdfFile.isFile() && pdfFile.getPath().getName().endsWith(".pdf")) {
                            pdfPaths.add(inputPath + "/" + categoryDir.getPath().getName() + "/" + pdfFile.getPath().getName());
                        }
                    }
                }
            }

            // Verificación y eliminación del archivo pdf_paths.txt si ya existe
            Path tempFile = new Path("pdf_paths.txt");
            if (fs.exists(tempFile)) { fs.delete(tempFile, true); }

            // Crear un archivo temporal con la lista de rutas de archivos PDF
            FSDataOutputStream outputStream = fs.create(tempFile);
            for (String pdfPath : pdfPaths) {
                outputStream.writeBytes(pdfPath + "\n");
            }
            outputStream.close();

            // Ejecuta el trabajo de MapReduce
            String[] args = {tempFile.toString(), outputPath, mensaje};
            boolean jobCompleted = PDFSearchJob.runJob(args);

            if (jobCompleted) {
                // Procesa los resultados
                FileStatus[] status = fs.listStatus(outputDir);

                respuesta = "Tu consulta se encuentra dentro de los libros:\n";
                logResultado("Tu consulta se encuentra dentro de los libros:\n");
                for (FileStatus fileStatus : status) {
                    if (fileStatus.isFile()) {
                        FSDataInputStream inputStream = fs.open(fileStatus.getPath());
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split("\t");
                            String bookTitle = parts[0];
                            String snippet = parts[1];

                            respuesta += bookTitle + "\n\"" + snippet + "\"\n-----\n";
                            // taChat.append(bookTitle + "\n\"" + snippet + "\"\n-----\n");
                        }

                        reader.close();
                    }
                }
                guardarConversacion(mensaje, respuesta);
            } else {
                logResultado("Error al ejecutar el trabajo de MapReduce.\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
            logResultado("Error al buscar en los libros.\n");
        }

        tfMensaje.setText("");
    }
    
    private void guardarConversacion(String mensajeUsuario, String respuestaSistema) {
        try {
            // Registrar el driver de SQlite JDBC
            Class.forName("org.sqlite.JDBC");

            // Configurar la URL de conexión para SQlite
            String url = "jdbc:sqlite:chat_history.db"; // Cambia el puerto/host si es necesario

            // Establecer conexión con SQlite
            try (Connection conn = DriverManager.getConnection(url);
                 Statement stmt = conn.createStatement()) {

                // Crear tabla si no existe
                String createTable = "CREATE TABLE IF NOT EXISTS chat_history (usuario TEXT, sistema TEXT)";
                stmt.execute(createTable);

                // Insertar datos en la tabla
                String insertQuery = String.format(
                    "INSERT INTO chat_history (usuario, sistema) VALUES ('%s', '%s')",
                    mensajeUsuario.replace("'", "''"), // Escapar comillas simples
                    respuestaSistema.replace("'", "''")
                );
                stmt.execute(insertQuery);

                // Confirmar resultado
                logResultado("Conversación guardada en SQLite.");
            }
        } catch (ClassNotFoundException ex) {
            logResultado("Error: No se encontró el driver SQLite JDBC: " + ex.getMessage());
        } catch (SQLException ex) {
            logResultado("Error al guardar conversación en SQLite: " + ex.getMessage());
        } catch (Exception ex) {
            logResultado("Error inesperado: " + ex.getMessage());
        }
    }
}
