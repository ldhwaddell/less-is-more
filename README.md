# less-is-more
A Rust implementation of [Less is More: Parameter-Free Text Classification with Gzip](https://arxiv.org/pdf/2212.09410)

## Results
| Num Records | Correctly Predicted | Incorrectly Predicted | Ratio   |   Time  |
|:-----------:|:-------------------:|-----------------------|---------|:-------:|
| 10          | 7                   | 3                     | 70      | 5.184   |
| 500         | 422                 | 78                    | 84.4    | 4:00.24 |
| 1000        | 846                 | 154                   | 84.6    | 8:30.93 |


## Notes
- Comparison still seems slow. More investigation into where time is being spent is needed. 