package Tools;

public class Mergesort {
        private long[] numbers;
        private long[] number_index;
        private long[] helper;
        private long[] helper_index;

        private int number;

        public void sort(long[] values, long[] indexes) {
                this.numbers = values;
                this.number_index = indexes;
                number = values.length;
                this.helper = new long[number];
                this.helper_index = new long[number];
                mergesort(0, number - 1);
        }

        private void mergesort(int low, int high) {
                // check if low is smaller than high, if not then the array is sorted
                if (low < high) {
                        // Get the index of the element which is in the middle
                        int middle = low + (high - low) / 2;
                        // Sort the left side of the array
                        mergesort(low, middle);
                        // Sort the right side of the array
                        mergesort(middle + 1, high);
                        // Combine them both
                        merge(low, middle, high);
                }
        }

        private void merge(int low, int middle, int high) {

                // Copy both parts into the helper array
                for (int i = low; i <= high; i++) {
                        helper[i] = numbers[i];
                        helper_index[i] = number_index[i];
                }

                int i = low;
                int j = middle + 1;
                int k = low;
                // Copy the smallest values from either the left or the right side back
                // to the original array
                while (i <= middle && j <= high) {
                        if (helper[i] <= helper[j]) {
                                numbers[k] = helper[i];
                                number_index[k] = helper_index[i];
                                i++;
                        } else {
                                numbers[k] = helper[j];
                                number_index[k] = helper_index[j];
                                j++;
                        }
                        k++;
                }
                // Copy the rest of the left side of the array into the target array
                while (i <= middle) {
                        numbers[k] = helper[i];
                        number_index[k] = helper_index[i];
                        k++;
                        i++;
                }

        }
}
