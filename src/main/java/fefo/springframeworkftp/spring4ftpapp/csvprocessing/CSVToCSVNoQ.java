package fefo.springframeworkftp.spring4ftpapp.csvprocessing;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;

@Component
public class CSVToCSVNoQ {

    public CSVToCSVNoQ() {
    }

    public void writeCSVfinal(String payload,@Header("new") String newCSV,@Header("old") String oldCsv) throws IOException {

        CSVReader reader = null;
        reader = new CSVReader(new FileReader(oldCsv));
        FileWriter fileWriter = new FileWriter(newCSV);

        //try (CSVWriter writer = new CSVWriter(new FileWriter(newCSV), ',', CSVWriter.NO_QUOTE_CHARACTER)) {
        try(CSVWriter csvWriter = new CSVWriter(fileWriter,CSVWriter.DEFAULT_SEPARATOR,
                                                            CSVWriter.NO_QUOTE_CHARACTER)){
            List<String[]> line;
            reader.readNext();
            reader.readNext();
            SimpleDateFormat from = new SimpleDateFormat("yyyy-mm-dd");
            SimpleDateFormat to = new SimpleDateFormat("ddMMMyyyy");

            line = reader.readAll();

            Iterator<String[]> itr = line.iterator();
            while (itr.hasNext()){
                String[] array = itr.next();
                if(array[0].equals("DET")) {
                    // System.out.println("Change Format " + to.format(from.parse(array[5])));
                    array[5] = to.format(from.parse(array[5]));
                }
            }

            while (itr.hasNext()){
                String[] array = itr.next();
                System.out.println("Line " + itr.next());
            }

            csvWriter.writeAll(line);
            csvWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
