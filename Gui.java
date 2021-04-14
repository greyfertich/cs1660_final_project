import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.nio.file.Files;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.nio.file.Paths;
import java.util.HashMap;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.*;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.api.services.dataproc.model.JobReference;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.*;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FileUtil;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Gui {

    private static JFrame frame;
    private static JPanel[][] panelHolder;
    private static JLabel filename_text, status_text;
    private static int n_rows = 4, n_cols = 3;
    private static Storage storage;
    private static GoogleCredentials credentials;
    private static boolean file_selected = false;
    private static String randomNumber = String.valueOf(new Random().nextInt(999999999));
    private static String bucket_name = "grey-fertich-bucket" + randomNumber;
    private static String bucket_location = "gs://" + bucket_name;
    private static JFileChooser fc = new JFileChooser();
    private static Job job;
    private static String projectId = "cs1600-gcp-project";
    private static String clusterId = "cluster-8881";
    private static String jobId = "job-" + randomNumber;
    private static HashMap<String, String> resultMap;

    public static void main(String[] args) {
        initGUI();
    }

    public static void showFileChooser() {
        fc = new JFileChooser();
        fc.setCurrentDirectory(new java.io.File("."));
        fc.setMultiSelectionEnabled(true);
        int retval = fc.showOpenDialog(null);
        if (retval == JFileChooser.APPROVE_OPTION) {
            File[] selectedFiles = fc.getSelectedFiles();
            String filenames = "<html>";
            for (File file : selectedFiles) {
                filenames += (filenames.length() > 6 ? "<br>" : "") + file.getName();
            }
            filename_text.setText(filenames + "</html>");
            filename_text.setVisible(true);
            System.out.println(filenames);
        }
    }

    public static boolean setupGCPAuth() {
        try {
//            GoogleCredentials creds = GoogleCredentials.fromStream(new FileInputStream("credentials.json"));
//            HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(creds);
//            Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer).build();
//            HadoopJob hJob = new HadoopJob();

            //credentials = GoogleCredentials.getApplicationDefault().createScoped(DataprocScopes.all());
            File f = new File("gcp_key.json");
            Scanner sc = new Scanner(f);
            System.out.println(sc.nextLine());
            credentials = GoogleCredentials.fromStream(new FileInputStream("gcp_key.json")).createScoped(DataprocScopes.all());
            if (credentials != null) {
                System.out.println("Got credentials");
            } else throw new Exception("Error getting credentials");

            storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
            if (storage != null) {
                System.out.println("Got storage");
            } else throw new Exception("Error getting storage");

            return true;
        } catch (Exception ex) {
            System.out.println("Error: Could not get credentials!");
        }

        return false;
    }

    public static boolean uploadFilesToGCP(File[] files) throws java.io.IOException {
        if (files.length == 0) {
            return true;
        }
        resetContentPane();
        status_text = new JLabel("Uploading Files to GCP");
        frame.getContentPane().add(status_text);


        Bucket bucket = storage.create(BucketInfo.of(bucket_name));
        System.out.println("created bucket");
        for (File file : files) {
            BlobId blobId = BlobId.of(bucket_name, "inputData/" + file.getName());
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
            Blob blob = storage.create(blobInfo, Files.readAllBytes(file.toPath()));
            System.out.println("Uploaded " + file.getName() + " to bucket " + bucket_name);
        }
        return true;

    }

    public static void runHadoopJob() throws InterruptedException, IOException {
        status_text.setText("Creating Hadoop Job");
        System.out.println("creating hadoop job");
        Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(),
                new HttpCredentialsAdapter(credentials)).setApplicationName("Grey Fertich Java Mapreduce Project").build();

        job = dataproc.projects().regions().jobs().submit(projectId, "us-central1",
                new SubmitJobRequest().setJob(new Job().setReference(new JobReference().setJobId(jobId))
                        .setPlacement(new JobPlacement().setClusterName(clusterId))
                        .setHadoopJob(new HadoopJob().setMainClass("MyHadoopJob")
                                .setJarFileUris(ImmutableList.of(
                                        "gs://grey-fertich-bucket/hj.jar"))
                                .setArgs(ImmutableList.of(
                                        bucket_location + "/inputData/",
                                        bucket_location + "/output-" + jobId)))))
                .execute();
        System.out.println("Running job");
        waitForJobToFinish(dataproc);
    }

    public static void waitForJobToFinish(Dataproc dataproc) throws InterruptedException, IOException {
        status_text.setText("Please wait... Job is running");
        Job job = dataproc.projects().regions().jobs().get(projectId, "us-central1", jobId).execute();
        while (job.getStatus().getState().compareTo("DONE") != 0) {
            System.out.println("Waiting for job to finish..., state=" + job.getStatus().getState());
            if (job.getStatus().getState().compareTo("ERROR") == 0) {
                System.out.println("Error: JOB NOT COMPLETED");
            }
            TimeUnit.SECONDS.sleep(1);
            job = dataproc.projects().regions().jobs().get(projectId, "us-central1", jobId).execute();
        }
        status_text.setText("Job complete");
        System.out.println("Job complete");
        getHadoopOutput();
    }

    public static void getHadoopOutput() throws IOException {
//        org.apache.hadoop.util.Shell.execCommand("hadoop fs -getmerge output-" + jobId + "/ hadoopOutput.txt");
        // delete current local output files
        int fileNumber = 0;
        File file = new File("hadoopOutput" + fileNumber + ".txt");
        while (file.exists()) {
            file.delete();
        }
        int partCounter = 0;
        Blob output = storage.get(BlobId.of(bucket_name, "output-" + jobId + "/part-r-0000" + partCounter));
        while (output != null) {
            output.downloadTo(Paths.get("hadoopOutput" + partCounter + ".txt"));
            partCounter++;
            output = storage.get(BlobId.of(bucket_name, "output-" + jobId + "/part-r-0000" + partCounter));
        }

        // Read in hadoop file and convert output back to a map
        Scanner outputScanner;
        resultMap = new HashMap<>();
        try {
            fileNumber = 0;
            file = new File("hadoopOutput" + fileNumber + ".txt");
            while (file.exists()) {
                outputScanner = new Scanner(file);

                while (outputScanner.hasNextLine()) {
                    String line = outputScanner.nextLine();
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        resultMap.put(parts[0].replaceAll("\\s+",""), parts[1].replaceAll("\\s+", ""));
                    }
                }
                fileNumber++;
                file = new File("hadoopOutput" + fileNumber + ".txt");
            }
            showHadoopOutput();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void showHadoopOutput() {
        resetContentPane();
        String[] columnNames = {"Word", "File", "Num. Occurrences"};
        String[][] data = new String[resultMap.size()][3];
        int row = 0;
        for (HashMap.Entry<String, String> entry : resultMap.entrySet()) {
            String word = entry.getKey();
            String[] vals = entry.getValue().split("-->");
            String filename = vals[0];
            String num_occurences = vals[1];
            data[row][0] = word;
            data[row][1] = filename;
            data[row++][2] = num_occurences;
        }
        JTable table = new JTable(data, columnNames);
        //table.scrollableViewportSize
        JScrollPane sp = new JScrollPane(table);
        frame.getContentPane().add(sp);
    }

    private static void initGUI() {
        frame = new JFrame("Aiden Fertich Search Engine");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(600, 400);

        panelHolder = new JPanel[4][3];
        frame.setLayout(new GridLayout(n_rows, n_cols));

        panelHolder[0][0] = new JPanel();

        JLabel text = new JLabel("Load My Engine");
        panelHolder[0][1] = new JPanel();
        panelHolder[0][1].add(text);

        insertPlaceholder(0,2);
        insertPlaceholder(1,0);

        JButton file_button = new JButton("Choose Files");
        file_button.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                showFileChooser();
                file_selected = true;
            }
        });

        panelHolder[1][1] = new JPanel();
        panelHolder[1][1].add(file_button);

        insertPlaceholder(1,2);
        insertPlaceholder(2,0);

        filename_text = new JLabel("");
        panelHolder[2][1] = new JPanel();
        panelHolder[2][1].add(filename_text);

        insertPlaceholder(2,2);
        insertPlaceholder(3,0);

        JButton index_button = new JButton("Construct Inverted Indices");
        index_button.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (file_selected) {
                    if (!setupGCPAuth()) {
                        System.out.println("\t\t---Error authenticating with Google Cloud!");
                    }
                    try {
                        if (!uploadFilesToGCP(fc.getSelectedFiles())) {
                            System.out.println("\t\t---Error uploading files to google cloud.");
                        }
                    } catch (java.io.IOException e1) {
                        System.out.println("Error: IOException");
                    }
                    try {
                        runHadoopJob();
                    } catch (InterruptedException | IOException interruptedException) {
                        interruptedException.printStackTrace();
                        System.out.println("Error running Hadoop Job!");
                    }
                }
            }
        });
        panelHolder[3][1] = new JPanel();
        panelHolder[3][1].add(index_button);

        insertPlaceholder(3,2);

        insertPanelsIntoFrame();
        frame.setVisible(true);
    }

    private static void insertPlaceholder(int row, int col) {
        panelHolder[row][col] = new JPanel();
    }

    private static void insertPanelsIntoFrame() {
        for (int r = 0; r < n_rows; r++) {
            for (int c = 0; c < n_cols; c++) {
                frame.getContentPane().add(panelHolder[r][c]);
            }
        }
    }

    private static void resetContentPane() {
        frame.getContentPane().removeAll();
        frame.getContentPane().revalidate();
        frame.getContentPane().repaint();
    }
}
