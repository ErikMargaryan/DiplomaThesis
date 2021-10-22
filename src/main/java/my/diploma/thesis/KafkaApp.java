package my.diploma.thesis;

import javafx.application.Application;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;
import my.diploma.thesis.consumer.KafkaStreaming;
import my.diploma.thesis.consumer.TwitterConsumer;
import my.diploma.thesis.producer.TwitterProducer;

import java.io.File;
import java.util.Arrays;
import java.util.stream.Collectors;

public class KafkaApp extends Application {

    private File selectedDir;
    private TwitterProducer thread1;
    private KafkaStreaming thread2;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage stage) {
        stage.setTitle("Twitter info");
        stage.setResizable(false);
        stage.getIcons().add(new Image("file:twitter_circle-512.png"));

        // layout1 content
        Label label1 = new Label("Please write the terms to find the tweets you want.");
        TextField terms = new TextField();
        terms.setPromptText("Terms to search with");

        // layout2 content
        Label label2 = new Label("Choose a name for the file and where you want to save it.");
        TextField filename = new TextField();
        filename.setPromptText("File name");

        Button browseButton = new Button("Select Folder");
        DirectoryChooser dir = new DirectoryChooser();
        dir.setInitialDirectory(new File("C:\\Users\\Karen_Aveyan\\Downloads"));

        // layout3 content
        Button startButton = new Button("Start the program");
        startButton.setDefaultButton(true);
        startButton.setManaged(true);
//        button.setPadding(new Insets(10, 10, 10, 10));
        Button stopButton = new Button("Stop");
        stopButton.setDisable(true);

        // layout 1
        VBox layout1 = new VBox(label1, terms);
        layout1.setAlignment(Pos.CENTER);
        layout1.setPrefWidth(350);

        // layout 2
        VBox layout2 = new VBox(label2, filename, browseButton);
        layout2.setAlignment(Pos.CENTER);
        layout2.setPrefWidth(350);

        // Layout 3
        HBox layout3 = new HBox(startButton, stopButton);
        layout3.setAlignment(Pos.CENTER);

        // Margins 1
        VBox.setMargin(label1, new Insets(5));
        VBox.setMargin(terms, new Insets(0, 20, 0, 20));

        // Margins 2
        VBox.setMargin(label2, new Insets(5));
        VBox.setMargin(filename, new Insets(0, 20, 0, 20));
        VBox.setMargin(browseButton, new Insets(10));

        // Margins 3
        HBox.setMargin(startButton, new Insets(10));
        HBox.setMargin(stopButton, new Insets(10));

        // Grid
        BorderPane borderPane = new BorderPane();
        borderPane.setLeft(layout1);
        borderPane.setRight(layout2);
        borderPane.setBottom(layout3);

        // Scene
        Scene scene = new Scene(borderPane);
        stage.setScene(scene);
        stage.show();

        // Actions
        startButton.setOnAction(e -> {
            if (terms.getText().trim().equals("")) {
                Alert invalidTerm = new Alert(Alert.AlertType.ERROR);
                invalidTerm.setTitle("Invalid Input");
                invalidTerm.setHeaderText("");
                invalidTerm.setContentText("\"Terms" + " field cannot be empty!");
                invalidTerm.showAndWait();
            } else if (filename.getText().isEmpty()) {
                Alert invalidTerm = new Alert(Alert.AlertType.ERROR);
                invalidTerm.setTitle("Invalid Input");
                invalidTerm.setHeaderText("");
                invalidTerm.setContentText("\"File name\"" + " field cannot be empty!");
                invalidTerm.showAndWait();
            } else if (selectedDir == null) {
                Alert invalidTerm = new Alert(Alert.AlertType.ERROR);
                invalidTerm.setTitle("Invalid Input");
                invalidTerm.setHeaderText("");
                invalidTerm.setContentText("Folder is not selected!");
                invalidTerm.showAndWait();
            } else {
                terms.setDisable(true);
                filename.setDisable(true);
                browseButton.setDisable(true);
                startButton.setDisable(true);
                thread1 = new TwitterProducer(Arrays.stream(terms.getText().split(","))
                        .map(String::trim)
                        .collect(Collectors.toList()));
                thread2 = new KafkaStreaming();
                thread1.start();
                thread2.start();
                stopButton.setDisable(false);
            }
        });

        browseButton.setOnAction(e -> selectedDir = dir.showDialog(stage));

        stopButton.setOnAction(e -> stopFetching(stage, filename.getText().trim(), stopButton));

        stage.setOnCloseRequest(e -> {
            if (!stopButton.isDisabled()) {
                e.consume();
                stopFetching(stage, filename.getText().trim(), stopButton);
            }
        });

    }

    private void stopFetching(Stage stage, String filename, Button button) {
        button.setDisable(true);
        thread1.terminate();
        thread2.terminate();

        Thread thread3 = new TwitterConsumer(selectedDir.getAbsolutePath(), filename);
        thread3.start();
        try {
            thread3.join();
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        stage.close();
    }
}
