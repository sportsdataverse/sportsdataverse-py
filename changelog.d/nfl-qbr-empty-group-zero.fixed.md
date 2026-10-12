- **NFL:** a quarterback with no sack, rush or penalty play gets 0 for that `exp_qbr` input, as the QBR trainer
  did, instead of 0/0 = NaN, which the booster read as missing. Spearman of `exp_qbr` vs ESPN Total QBR on the
  2023 slice rises from 0.786 to 0.810. (#802)
