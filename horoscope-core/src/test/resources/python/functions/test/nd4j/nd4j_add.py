def nd4j_add(a, b):
    from org.nd4j.linalg.factory import Nd4j;
    ones = Nd4j.create(3, 2)
    ones.addi(a)
    ones = ones.add(b)
    return ones.toDoubleMatrix()