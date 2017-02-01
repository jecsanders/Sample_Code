// Connor Sanders
// Sample Java Code


public class LoanPaymentSchedule {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//Declare Input Variables
		int numOfYears;
		double loanAmount;
		double annualInterestRate;
		
		//Create Scanner object
		java.util.Scanner input = new java.util.Scanner(System.in);
		
		//Prompt user for Inputs
		System.out.print("Enter loan amount, for example 120000.95: ");
		loanAmount = input.nextDouble();
		
		System.out.print("Enter number of  years as an integer, for example 5: ");
		numOfYears = input.nextInt();
		
		System.out.print("Enter yearly interest rate, for example 8.25: ");
		annualInterestRate = input.nextDouble();
		
		//Calculate monthly interest rate and payment
		double monthlyInterestRate = annualInterestRate / 1200;
		
		double monthlyRepayment = loanAmount * monthlyInterestRate / 
				(1 - (Math.pow(1 / (1+monthlyInterestRate), numOfYears*12)));
		
		//Declare output variables
		double balance = loanAmount;
		double interest;
		double principal;
		
		//Print loan parameters to console
		System.out.println("Loan Amount $" + loanAmount);
		System.out.println("Number of years: " + numOfYears);
		System.out.println("Interest Rate: " + annualInterestRate + "%");
		System.out.println();
		System.out.println("Monthly Payment: $" + (int) (monthlyRepayment * 100) / 100.0);
		System.out.println("Total Payment $" + (int) (monthlyRepayment * 12 * numOfYears * 100) / 100.0+"\n");
		
		//Print loan payment schedule table to console
		System.out.println("Payment#\tInterest\tPrincipal\tBalance");
		
		for (int i = 1; i <= numOfYears*12; i++) {
	
		interest = (int) (monthlyInterestRate * balance * 100) / 100.0;
		principal = (int) ((monthlyRepayment - interest) * 100) / 100.0;
		balance = (int) ((balance - principal) * 100) / 100.0;
		System.out.println(i + "\t\t" + interest + "\t\t" + principal + "\t\t" + balance);
		
		}
		
	}

}
