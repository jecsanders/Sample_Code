// Connor Sanders
// Sample Java Code


import java.util.Scanner;

public class SubtractionQuizLoop {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		final int NUMBER_OF_QUESTIONS = 5; //number of questions to ask
		int correctCount = 0;
		int count = 0;
		long startTime = System.currentTimeMillis();
		String output = "";
		Scanner input = new Scanner(System.in);
		
		while (count < NUMBER_OF_QUESTIONS) {
			
			//generate 2 numbers
			int number1 = (int) (Math.random() * 10);
			int number2 = (int) (Math.random() * 10);
			
			// swap if needed
			if (number1 < number2) {
				int temp = number1;
				number1 = number2;
				number2 = temp;
			}
			
			System.out.print(
					"What is " + number1 + " - " + number2 + "? ");
			int answer = input.nextInt();
			
			if (number1 - number2 == answer) {
				System.out.println("You are correct!");
				correctCount++;
			}
			
			else {
				System.out.println("You answer is wrong.\n" + number1 
				+ " - " + number2 + " should be " + (number1-number2));
			}
			count++;
			
			output += "/n" + number1 + "-" + number2 + "-" + answer + 
					((number1 - number2 == answer) ? " correct" : " wrong");
			
		}
		
		long endTime = System.currentTimeMillis();
		long testTime = endTime - startTime;
		
		System.out.println("Correct count is " + correctCount +
				"\nTest time is " + testTime/1000 + " seconds\n");
		
	}

}
